"""Size-tiered compaction correctness: newest wins, tombstones handled."""

from __future__ import annotations

from lsmkv import DB


def test_compaction_preserves_latest_and_hides_tombstones(tmp_path):
    db = DB(tmp_path, memtable_bytes=4096, level0_size=4)
    # Round 1: 600 keys across many segments.
    for i in range(600):
        db.put(f"k{i:05d}".encode(), f"v{i}".encode())
    db.wait_for_compactions(timeout=10)

    # Overwrite half and delete a quarter after compaction settled.
    for i in range(0, 600, 2):
        db.put(f"k{i:05d}".encode(), b"new")
    for i in range(1, 600, 4):
        db.delete(f"k{i:05d}".encode())
    db.wait_for_compactions(timeout=10)
    # Force several passes so everything reaches the deepest tier and
    # tombstones get physically dropped.
    for _ in range(10):
        if not db.compact_now():
            break
        db.wait_for_compactions(timeout=10)

    for i in range(600):
        k = f"k{i:05d}".encode()
        got = db.get(k)
        if i % 4 == 1:
            assert got is None, k
        elif i % 2 == 0:
            assert got == b"new", k
        else:
            assert got == f"v{i}".encode(), k

    scanned = list(db.scan(b"k"))
    expected = sum(1 for i in range(600) if i % 4 != 1)
    assert len(scanned) == expected
    keys = [k for k, _ in scanned]
    assert keys == sorted(keys)
    db.close()

    db = DB(tmp_path)
    for i in range(600):
        k = f"k{i:05d}".encode()
        got = db.get(k)
        if i % 4 == 1:
            assert got is None
        elif i % 2 == 0:
            assert got == b"new"
        else:
            assert got == f"v{i}".encode()
    db.close()


def test_compaction_does_not_block_writes(tmp_path):
    import threading
    import time

    db = DB(tmp_path, memtable_bytes=16 * 1024, level0_size=4, sync_on_commit=False)
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            db.put(f"c{i:07d}".encode(), b"x" * 200)
            i += 1

    threads = [threading.Thread(target=writer) for _ in range(3)]
    for t in threads:
        t.start()
    time.sleep(3)
    stop.set()
    for t in threads:
        t.join()
    n = sum(1 for _ in db.scan(b"c"))
    assert n > 100
    db.close()
