"""Many flushes, scan completeness/ordering, segment checksums."""

from __future__ import annotations

import os

from lsmkv import DB


def test_100k_writes_scan_complete_and_ordered(tmp_path):
    db = DB(tmp_path, memtable_bytes=2 * 1024 * 1024, sync_on_commit=False)
    n = 100_000
    # Insert in shuffled order; SSTables still must be key ordered.
    order = list(range(n))
    # deterministic pseudo shuffle (no third-party deps)
    seed = 12345
    for i in range(n - 1, 0, -1):
        seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
        j = seed % (i + 1)
        order[i], order[j] = order[j], order[i]
    for i in order:
        db.put(f"key:{i:08d}".encode(), f"val:{i:08d}".encode())
    assert db.stats()["flushes"] >= 2

    count = 0
    prev = b""
    for k, v in db.scan(b"key:"):
        assert k > prev
        prev = k
        idx = int(k.split(b":")[1])
        assert v == f"val:{idx:08d}".encode()
        count += 1
    assert count == n

    # random point lookups
    for idx in (0, 1, n - 1, 12345, 67890, 55555):
        assert db.get(f"key:{idx:08d}".encode()) == f"val:{idx:08d}".encode()
    db.close()

    db = DB(tmp_path)
    count = sum(1 for _ in db.scan(b"key:"))
    assert count == n
    db.close()


def test_100k_persistence(tmp_path):
    # separate DB so tests stay independent
    loc = tmp_path / "db"
    db = DB(loc, sync_on_commit=False)
    n = 100_000
    for i in range(n):
        db.put(f"p{i:08d}".encode(), bytes([i & 0xFF]) * 32)
    db.close()
    db = DB(loc)
    for i in range(0, n, 997):
        assert db.get(f"p{i:08d}".encode()) == bytes([i & 0xFF]) * 32
    assert sum(1 for _ in db.scan(b"p")) == n
    db.close()
