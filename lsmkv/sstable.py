"""Sorted String Table (SSTable) segment files.

File layout (all integers little-endian)::

    [data block 0][block crc32]
    [data block 1][block crc32]
    ...
    [index][index crc32]
    [footer: 40 bytes, fixed size, at end of file]

A *data block* holds up to ``INDEX_EVERY`` (64) entries, sorted by key::

    entry := key_len u32 | value_len u32 | seqno u64 | flags u8 | key | value
    (flags bit0 = tombstone; identical to the WAL entry body)

Each block is followed by the crc32 of its raw bytes.

The *index* is sparse: one entry per block, holding the block's first key::

    index := count u32
             { key_len u32 | block_offset u64 | block_len u32 | key } * count
             crc32 u32            (crc of all preceding index bytes)

``block_len`` includes the block's trailing 4-byte crc.

The *footer* is fixed-size so it can be found by seeking from the end::

    index_offset u64 | index_len u64 | entry_count u64 | max_seqno u64 | magic 8s
"""

from __future__ import annotations

import bisect
import os
import struct
import zlib
from typing import Iterator, List, NamedTuple, Optional, Tuple

INDEX_EVERY = 64  # one sparse-index entry per 64 keys
MAGIC = b"LSMKVST1"
ENTRY_HEADER = struct.Struct("<IIQB")  # key_len, value_len, seqno, flags
FOOTER = struct.Struct("<QQQQ8s")
FOOTER_SIZE = FOOTER.size
FLAG_TOMBSTONE = 0x01


class SstEntry(NamedTuple):
    key: bytes
    value: bytes
    seqno: int
    tombstone: bool


class CorruptionError(Exception):
    pass


class SstableWriter:
    """Builds one segment file.  Keys must be added in ascending order."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._f = open(path, "wb")
        self._offset = 0
        self._index: List[Tuple[bytes, int, int]] = []  # (first_key, offset, len)
        self._block = bytearray()
        self._block_entries = 0
        self._block_first_key: Optional[bytes] = None
        self._block_offset = 0
        self.entry_count = 0
        self.max_seqno = 0

    def add(self, key: bytes, value: bytes, seqno: int, tombstone: bool) -> None:
        if self._block_entries == 0:
            self._block_first_key = key
            self._block_offset = self._offset
        flags = FLAG_TOMBSTONE if tombstone else 0
        self._block += ENTRY_HEADER.pack(len(key), len(value), seqno, flags)
        self._block += key
        self._block += value
        self._block_entries += 1
        self.entry_count += 1
        if seqno > self.max_seqno:
            self.max_seqno = seqno
        if self._block_entries >= INDEX_EVERY:
            self._flush_block()

    def _flush_block(self) -> None:
        block = bytes(self._block)
        block += struct.pack("<I", zlib.crc32(block))
        self._f.write(block)
        assert self._block_first_key is not None
        self._index.append((self._block_first_key, self._block_offset, len(block)))
        self._offset += len(block)
        self._block = bytearray()
        self._block_entries = 0

    def finish(self) -> None:
        if self._block_entries:
            self._flush_block()
        index_offset = self._offset
        index = bytearray(struct.pack("<I", len(self._index)))
        for key, offset, length in self._index:
            index += struct.pack("<IQI", len(key), offset, length)
            index += key
        index += struct.pack("<I", zlib.crc32(index))
        self._f.write(index)
        self._f.write(
            FOOTER.pack(index_offset, len(index), self.entry_count, self.max_seqno, MAGIC)
        )
        self._f.flush()
        os.fsync(self._f.fileno())
        self._f.close()


def _parse_block(block: bytes) -> List[Tuple[bytes, bytes, int, bool]]:
    if len(block) < 4:
        raise CorruptionError("block too short")
    data, (crc,) = block[:-4], struct.unpack("<I", block[-4:])
    if zlib.crc32(data) != crc:
        raise CorruptionError("block crc mismatch")
    entries: List[Tuple[bytes, bytes, int, bool]] = []
    pos = 0
    while pos < len(data):
        key_len, value_len, seqno, flags = ENTRY_HEADER.unpack_from(data, pos)
        pos += ENTRY_HEADER.size
        key = data[pos : pos + key_len]
        pos += key_len
        value = data[pos : pos + value_len]
        pos += value_len
        entries.append((key, value, seqno, bool(flags & FLAG_TOMBSTONE)))
    return entries


class SstableReader:
    """Reads one segment file.  The file stays open for the reader's
    lifetime, so unlinking the path (after compaction) never breaks an
    in-flight reader."""

    def __init__(self, path: str, segment_id: int, tier: int) -> None:
        self.path = path
        self.segment_id = segment_id
        self.tier = tier
        # os.pread gives atomic positional reads, so concurrent readers on
        # the same segment need no locking.
        self._fd = os.open(path, os.O_RDONLY)
        self._closed = False
        file_size = os.path.getsize(path)
        if file_size < FOOTER_SIZE:
            raise CorruptionError(f"{path}: file too small")
        index_offset, index_len, self.entry_count, self.max_seqno, magic = FOOTER.unpack(
            self._read_at(file_size - FOOTER_SIZE, FOOTER_SIZE)
        )
        if magic != MAGIC:
            raise CorruptionError(f"{path}: bad magic")
        index = self._read_at(index_offset, index_len)
        if len(index) != index_len:
            raise CorruptionError(f"{path}: short index")
        body, (crc,) = index[:-4], struct.unpack("<I", index[-4:])
        if zlib.crc32(body) != crc:
            raise CorruptionError(f"{path}: index crc mismatch")
        (count,) = struct.unpack_from("<I", body, 0)
        pos = 4
        self._index_keys: List[bytes] = []
        self._index_blocks: List[Tuple[int, int]] = []
        for _ in range(count):
            key_len, offset, block_len = struct.unpack_from("<IQI", body, pos)
            pos += 16
            key = body[pos : pos + key_len]
            pos += key_len
            self._index_keys.append(key)
            self._index_blocks.append((offset, block_len))

    def _read_at(self, offset: int, length: int) -> bytes:
        chunks = []
        remaining = length
        while remaining > 0:
            chunk = os.pread(self._fd, remaining, offset)
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_block(self, offset: int, length: int) -> List[Tuple[bytes, bytes, int, bool]]:
        block = self._read_at(offset, length)
        if len(block) != length:
            raise CorruptionError(f"{self.path}: short block read")
        return _parse_block(block)

    def get(self, key: bytes) -> Optional[SstEntry]:
        """Return the entry for ``key`` or ``None`` if not in this segment."""
        if not self._index_keys:
            return None
        i = bisect.bisect_right(self._index_keys, key) - 1
        if i < 0:
            return None
        offset, length = self._index_blocks[i]
        entries = self._read_block(offset, length)
        keys = [e[0] for e in entries]
        j = bisect.bisect_left(keys, key)
        if j < len(keys) and keys[j] == key:
            k, value, seqno, tombstone = entries[j]
            return SstEntry(k, value, seqno, tombstone)
        return None

    def iter_entries(self) -> Iterator[SstEntry]:
        """Yield every entry in key order (sequential scan)."""
        for offset, length in self._index_blocks:
            for key, value, seqno, tombstone in self._read_block(offset, length):
                yield SstEntry(key, value, seqno, tombstone)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self._fd)
        except OSError:
            pass
        self._fd = -1

    def __del__(self) -> None:
        self.close()
