"""Sorted string table segment files.

Layout (all integers little endian):

    +-----------------------+  offset 0
    | data blocks           |
    |   block := entries*   |  each entry: <Q seq><I klen><I vlen><B flags>
    |           <I crc32>   |              <klen bytes key><vlen bytes value>
    +-----------------------+  block trailer: uint32 crc32 of the block body
    | index region          |  one entry per block:
    |   index entries*      |    <Q block_offset><I block_len>
    |                       |    <I first_key_len><first_key bytes>
    |   <I index_crc32>     |
    +-----------------------+
    | footer (fixed prefix +|
    |  variable min/max key)|
    +-----------------------+

A sparse index point is emitted at the start of every block (blocks hold at
most 64 entries), giving one index item per 64 keys.
"""

from __future__ import annotations

import collections
import os
import struct
import threading
import zlib
from pathlib import Path
from typing import Iterator

from .types import Entry

MAGIC = 0x53535431  # "SST1"
KEYS_PER_BLOCK = 64
BLOCK_CACHE_CAPACITY = 4096  # decoded blocks per SSTable

_ENTRY_HDR = struct.Struct("<QIIB")  # seq, klen, vlen, flags
_CRC32 = struct.Struct("<I")
_IDX_HDR = struct.Struct("<QII")  # block_offset, block_len, first_key_len
# Footer fixed prefix: magic, version, index_end, index_len,
#                      index_region_start, entry_count, min_key_len, max_key_len
_FOOTER_PREFIX = struct.Struct("<IIQQQIII")
FOOTER_PREFIX_SIZE = _FOOTER_PREFIX.size  # 36


class SSTCorruptError(Exception):
    """Raised when a segment fails structural or checksum validation."""


def _encode_entry(e: Entry) -> bytes:
    return (
        _ENTRY_HDR.pack(e.sequence, len(e.key), len(e.value), 1 if e.is_tombstone else 0)
        + e.key
        + e.value
    )


def _decode_entries(body: bytes) -> list[Entry]:
    out: list[Entry] = []
    pos = 0
    n = len(body)
    while pos < n:
        seq, klen, vlen, flags = _ENTRY_HDR.unpack_from(body, pos)
        pos += _ENTRY_HDR.size
        key = body[pos : pos + klen]
        pos += klen
        val = body[pos : pos + vlen]
        pos += vlen
        out.append(Entry(key, val, seq, bool(flags & 1)))
    return out


class SSTWriter:
    """Writes entries (already in ascending key order) into a segment."""

    def __init__(self, level: int, table_id: int, directory: Path) -> None:
        self.level = level
        self.table_id = table_id
        self.path = directory / f"seg-{table_id:08d}.sst"
        self._tmp = directory / f"seg-{table_id:08d}.tmp"
        self._f = open(self._tmp, "wb", buffering=0)
        self._block: list[bytes] = []
        self._block_count = 0
        self._block_offset = 0
        self._index: list[bytes] = []
        self._block_first_key: bytes | None = None
        self.entry_count = 0
        self.min_key: bytes | None = None
        self.max_key: bytes | None = None
        self._finished = False

    def _writeall(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = self._f.write(view)
            if not written:
                raise OSError("short write to segment")
            view = view[written:]

    def _flush_block(self, first_key: bytes) -> None:
        body = b"".join(self._block)
        crc = zlib.crc32(body) & 0xFFFFFFFF
        self._index.append(
            _IDX_HDR.pack(self._block_offset, len(body), len(first_key)) + first_key
        )
        self._writeall(body + _CRC32.pack(crc))
        self._block_offset += len(body) + _CRC32.size
        self._block = []
        self._block_count += 1

    def add(self, entry: Entry) -> None:
        if self._finished:
            raise RuntimeError("writer already finished")
        if self.min_key is None:
            self.min_key = entry.key
        self.max_key = entry.key
        if self._block and len(self._block) % KEYS_PER_BLOCK == 0:
            # Current block reached 64 entries; close it before appending.
            assert self._block_first_key is not None
            self._flush_block(self._block_first_key)
        if not self._block:
            # First entry of a new block -> sparse-index point.
            self._block_first_key = entry.key
        self._block.append(_encode_entry(entry))
        self.entry_count += 1

    def finish(self) -> Path:
        if self._finished:
            return self.path
        if self._block:
            assert self._block_first_key is not None
            self._flush_block(self._block_first_key)
        index_region_start = self._block_offset
        index_body = b"".join(self._index)
        index_crc = zlib.crc32(index_body) & 0xFFFFFFFF
        self._writeall(index_body + _CRC32.pack(index_crc))
        index_len = len(index_body) + _CRC32.size
        index_end = index_region_start + index_len
        assert self.min_key is not None and self.max_key is not None
        footer_prefix = _FOOTER_PREFIX.pack(
            MAGIC,
            1,
            index_end,
            index_len,
            index_region_start,
            self.entry_count,
            len(self.min_key),
            len(self.max_key),
        )
        footer_suffix = (
            self.min_key
            + self.max_key
            + _CRC32.pack(zlib.crc32(footer_prefix + self.min_key + self.max_key) & 0xFFFFFFFF)
            + struct.pack("<I", FOOTER_PREFIX_SIZE + len(self.min_key) + len(self.max_key) + 8)
        )
        self._writeall(footer_prefix + footer_suffix)
        self._f.flush()
        os.fsync(self._f.fileno())
        self._f.close()
        os.replace(self._tmp, self.path)
        self._finished = True
        return self.path


class SSTable:
    """Reader for one segment.  Keeps its file descriptor open and uses pread
    so concurrent readers never disturb each other's offsets."""

    def __init__(self, path: Path) -> None:
        self.path = path
        name = path.name
        self.table_id = int(name[len("seg-") : -len(".sst")])
        self._f = os.open(path, os.O_RDONLY)
        self.size = os.fstat(self._f).st_size
        self._block_cache: collections.OrderedDict[int, list[Entry]] = collections.OrderedDict()
        self._cache_lock = threading.Lock()
        self._read_footer()
        self._read_index()

    def _pread(self, offset: int, length: int) -> bytes:
        chunks: list[bytes] = []
        remaining = length
        while remaining:
            chunk = os.pread(self._f, remaining, offset)
            if not chunk:
                raise SSTCorruptError(f"short read in {self.path}")
            chunks.append(chunk)
            offset += len(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_footer(self) -> None:
        if self.size < 4:
            raise SSTCorruptError(f"{self.path} too small for footer")
        flen = struct.unpack("<I", self._pread(self.size - 4, 4))[0]
        if flen > self.size or flen < FOOTER_PREFIX_SIZE + 8:
            raise SSTCorruptError(f"{self.path} bad footer length {flen}")
        footer = self._pread(self.size - flen, flen - 4)
        (
            magic,
            version,
            self.index_end,
            self.index_len,
            self.index_region_start,
            self.entry_count,
            min_len,
            max_len,
        ) = _FOOTER_PREFIX.unpack_from(footer, 0)
        if magic != MAGIC or version != 1:
            raise SSTCorruptError(f"{self.path} bad magic/version")
        keys = footer[_FOOTER_PREFIX.size : _FOOTER_PREFIX.size + min_len + max_len]
        crc_stored = struct.unpack_from(
            "<I", footer, _FOOTER_PREFIX.size + min_len + max_len
        )[0]
        crc_body = footer[: _FOOTER_PREFIX.size + min_len + max_len]
        if zlib.crc32(crc_body) & 0xFFFFFFFF != crc_stored:
            raise SSTCorruptError(f"{self.path} footer CRC mismatch")
        self.min_key = keys[:min_len]
        self.max_key = keys[min_len : min_len + max_len]

    def _read_index(self) -> None:
        region = self._pread(self.index_region_start, self.index_len)
        body, crc_bytes = region[:-4], region[-4:]
        if zlib.crc32(body) & 0xFFFFFFFF != struct.unpack("<I", crc_bytes)[0]:
            raise SSTCorruptError(f"{self.path} index CRC mismatch")
        items: list[tuple[bytes, int, int]] = []
        pos = 0
        while pos < len(body):
            offset, blen, klen = _IDX_HDR.unpack_from(body, pos)
            pos += _IDX_HDR.size
            first_key = body[pos : pos + klen]
            pos += klen
            items.append((first_key, offset, blen))
        self._index_items = items

    def _read_block(self, offset: int, blen: int) -> list[Entry]:
        with self._cache_lock:
            cached = self._block_cache.get(offset)
            if cached is not None:
                self._block_cache.move_to_end(offset)
                return cached
        raw = self._pread(offset, blen + 4)
        body, crc_bytes = raw[:-4], raw[-4:]
        if zlib.crc32(body) & 0xFFFFFFFF != struct.unpack("<I", crc_bytes)[0]:
            raise SSTCorruptError(f"{self.path} data block CRC mismatch at {offset}")
        entries = _decode_entries(body)
        with self._cache_lock:
            self._block_cache[offset] = entries
            self._block_cache.move_to_end(offset)
            while len(self._block_cache) > BLOCK_CACHE_CAPACITY:
                self._block_cache.popitem(last=False)
        return entries

    def get(self, key: bytes) -> Entry | None:
        if key < self.min_key or key > self.max_key:
            return None
        # Sparse-index locate: rightmost block whose first_key <= target.
        lo, hi = 0, len(self._index_items) - 1
        chosen = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if self._index_items[mid][0] <= key:
                chosen = mid
                lo = mid + 1
            else:
                hi = mid - 1
        _, offset, blen = self._index_items[chosen]
        entries = self._read_block(offset, blen)
        # Binary search inside the block (entries are key ordered).
        lo, hi = 0, len(entries) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if entries[mid].key < key:
                lo = mid + 1
            elif entries[mid].key > key:
                hi = mid - 1
            else:
                return entries[mid]
        return None

    def entries(self) -> Iterator[Entry]:
        for _, offset, blen in self._index_items:
            yield from self._read_block(offset, blen)

    def iter_from(self, start_key: bytes) -> Iterator[Entry]:
        """Yield entries with key >= ``start_key`` in ascending order."""
        if start_key > self.max_key:
            return
        lo, hi = 0, len(self._index_items) - 1
        chosen = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if self._index_items[mid][0] <= start_key:
                chosen = mid
                lo = mid + 1
            else:
                hi = mid - 1
        # First block may begin before start_key; skip its prefix. Every
        # later block's first key is strictly greater, so yield it wholesale.
        _, offset, blen = self._index_items[chosen]
        for entry in self._read_block(offset, blen):
            if entry.key >= start_key:
                yield entry
        for _, offset, blen in self._index_items[chosen + 1 :]:
            for entry in self._read_block(offset, blen):
                yield entry

    def close(self) -> None:
        os.close(self._f)
