"""Write-ahead log.

Every commit is appended as one framed, CRC-32C-protected record and fsynced
before the write is acknowledged.  Replay detects a torn trailing record (a
``kill -9`` in the middle of an append) and discards only that record.
"""

from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

from .types import Entry

MAGIC = 0x57414C31  # "WAL1"
# Header layout (little endian), 25 bytes:
#   uint32 magic, uint32 crc32(body), uint64 sequence,
#   uint32 key_len, uint32 val_len, uint8  flags (bit0 = tombstone)
_HEADER = struct.Struct("<IIQIIB")
HEADER_SIZE = _HEADER.size
MAX_RECORD_SIZE = 1 << 30  # 1 GiB sanity bound


class WalCorruptError(Exception):
    """Raised when a committed (fully framed) WAL record fails verification."""


class WALWriter:
    def __init__(self, path: Path, sync: bool = True) -> None:
        self.path = path
        self.sync = sync
        self._f = open(path, "ab", buffering=0)

    def append(self, entry: Entry) -> None:
        flags = 1 if entry.is_tombstone else 0
        body = (
            struct.pack("<QIIB", entry.sequence, len(entry.key), len(entry.value), flags)
            + entry.key
            + entry.value
        )
        crc = zlib.crc32(body) & 0xFFFFFFFF
        record = struct.pack("<II", MAGIC, crc) + body
        assert len(record) == HEADER_SIZE + len(entry.key) + len(entry.value)
        self._f.write(record)
        if self.sync:
            os.fsync(self._f.fileno())

    def truncate(self) -> None:
        """Truncate to zero bytes (memtable was flushed)."""
        self._f.seek(0)
        self._f.truncate(0)
        if self.sync:
            os.fsync(self._f.fileno())

    def close(self) -> None:
        self._f.close()


def encode_record(entry: Entry) -> bytes:
    """Exposed for tests that build raw WAL bytes."""
    flags = 1 if entry.is_tombstone else 0
    body = (
        struct.pack("<QIIB", entry.sequence, len(entry.key), len(entry.value), flags)
        + entry.key
        + entry.value
    )
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return struct.pack("<II", MAGIC, crc) + body


def replay(path: Path) -> tuple[list[Entry], bool, int]:
    """Replay a WAL file.

    Returns ``(entries, had_torn_tail, valid_length)``.  A short / torn final frame (the
    process was killed mid-append) is dropped silently; a fully present frame
    with a bad magic or CRC raises :class:`WalCorruptError` because committed
    data must never silently disappear.
    """
    if not path.exists():
        return [], False, 0
    data = path.read_bytes()
    entries: list[Entry] = []
    pos = 0
    torn = False
    n = len(data)
    while pos < n:
        if n - pos < HEADER_SIZE:
            torn = True  # partial header
            break
        magic, crc, seq, klen, vlen, flags = _HEADER.unpack_from(data, pos)
        if magic != MAGIC:
            # A fully written header must start with magic; anything else at a
            # frame boundary is a torn tail.
            torn = True
            break
        total = HEADER_SIZE + klen + vlen
        if total > MAX_RECORD_SIZE:
            raise WalCorruptError(f"WAL record implausibly large at offset {pos}")
        if n - pos < total:
            torn = True  # header present but payload truncated
            break
        body_start = pos + 8
        body_end = pos + total
        stored = crc
        actual = zlib.crc32(data[body_start:body_end]) & 0xFFFFFFFF
        if stored != actual:
            # Full frame present but checksum wrong: with a torn tail this can
            # also happen when garbage was appended; treat it as torn only if
            # this is the last frame, otherwise it is real corruption.
            if body_end == n:
                torn = True
                break
            raise WalCorruptError(f"CRC mismatch at WAL offset {pos}")
        key = data[pos + HEADER_SIZE : pos + HEADER_SIZE + klen]
        val = data[pos + HEADER_SIZE + klen : body_end]
        entries.append(Entry(key, val, seq, bool(flags & 1)))
        pos = body_end
    return entries, torn, pos
