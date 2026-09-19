"""Write-ahead log with per-record crc32 checksums.

Record layout (all integers little-endian)::

    offset  field       size
    0       crc32       4      crc32 of bytes [4, record_end)
    4       key_len     4      uint32
    8       value_len   4      uint32
    12      seqno       8      uint64, monotonically increasing
    20      flags       1      bit0 = tombstone
    21      key         key_len
    21+kl   value       value_len

Recovery reads records sequentially.  A short read or a crc mismatch means
the tail of the file was torn (e.g. by ``kill -9``); the file is truncated
at the start of the bad record and every complete record before it is kept.
"""

from __future__ import annotations

import os
import struct
import zlib
from typing import Iterator, NamedTuple, Tuple

HEADER = struct.Struct("<IIQ")  # key_len, value_len, seqno  (after crc)
CRC_SIZE = 4
FLAG_SIZE = 1
FLAG_TOMBSTONE = 0x01
PREFIX_SIZE = CRC_SIZE + HEADER.size + FLAG_SIZE  # 4 + 16 + 1 = 21


class WalEntry(NamedTuple):
    key: bytes
    value: bytes
    seqno: int
    tombstone: bool


def encode_record(key: bytes, value: bytes, seqno: int, tombstone: bool) -> bytes:
    flags = FLAG_TOMBSTONE if tombstone else 0
    body = HEADER.pack(len(key), len(value), seqno) + bytes([flags]) + key + value
    return struct.pack("<I", zlib.crc32(body)) + body


class WalWriter:
    """Appends records to one WAL file; every write is fsync'd."""

    def __init__(self, path: str) -> None:
        self.path = path
        # O_APPEND so a crashed writer can never leave a hole in the file.
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    def append(self, key: bytes, value: bytes, seqno: int, tombstone: bool) -> None:
        data = encode_record(key, value, seqno, tombstone)
        os.write(self._fd, data)
        os.fsync(self._fd)

    def close(self) -> None:
        if self._fd < 0:
            return
        try:
            os.close(self._fd)
        except OSError:
            pass
        self._fd = -1

    def __del__(self) -> None:
        self.close()


def replay(path: str, truncate_garbage: bool = True) -> Iterator[WalEntry]:
    """Yield complete records from ``path`` in order.

    Stops at the first incomplete or corrupt record; if ``truncate_garbage``
    is true the file is truncated to the end of the last good record.
    """
    good_end = 0
    with open(path, "rb") as f:
        while True:
            start = good_end
            prefix = f.read(PREFIX_SIZE)
            if len(prefix) == 0:
                break
            if len(prefix) < PREFIX_SIZE:
                break  # torn header
            (crc,) = struct.unpack("<I", prefix[:CRC_SIZE])
            key_len, value_len, seqno = HEADER.unpack(prefix[CRC_SIZE : CRC_SIZE + HEADER.size])
            flags = prefix[CRC_SIZE + HEADER.size]
            payload_len = key_len + value_len
            payload = f.read(payload_len)
            if len(payload) < payload_len:
                break  # torn payload
            body = prefix[CRC_SIZE:] + payload
            if zlib.crc32(body) != crc:
                break  # corrupt record
            good_end = start + PREFIX_SIZE + payload_len
            yield WalEntry(
                key=payload[:key_len],
                value=payload[key_len:],
                seqno=seqno,
                tombstone=bool(flags & FLAG_TOMBSTONE),
            )
    file_size = os.path.getsize(path)
    if truncate_garbage and file_size > good_end:
        with open(path, "r+b") as f:
            f.truncate(good_end)
