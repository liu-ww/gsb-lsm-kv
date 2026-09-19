"""Large-volume test: 100k writes trigger several flushes and compactions;
scan must stay ordered and complete."""

import tempfile

from lsmkv import DB

N = 100_000


def _key(i: int) -> bytes:
    return b"user:%08d" % i


def _val(i: int) -> bytes:
    return b"payload-%d-" % i + b"x" * 50


def test_100k_writes_flush_and_scan():
    with tempfile.TemporaryDirectory() as path:
        # 512KiB memtable -> many flushes; fanout 4 -> compactions happen.
        db = DB(path, memtable_threshold=512 * 1024)
        for i in range(N):
            db.put(_key(i), _val(i))
        db.flush()
        db.wait_for_background()
        assert db.segment_count >= 1

        # random-access reads
        for i in range(0, N, 9973):
            assert db.get(_key(i)) == _val(i)

        # full scan: ordered + complete
        count = 0
        prev = None
        for key, value in db.scan(b"user:"):
            if prev is not None:
                assert key > prev, "scan out of order"
            prev = key
            expected = _val(count)
            assert key == _key(count)
            assert value == expected
            count += 1
        assert count == N

        # overwrite every 10th key, delete every 10th+5, verify via scan
        for i in range(0, N, 10):
            db.put(_key(i), b"new-" + _val(i))
        for i in range(5, N, 10):
            db.delete(_key(i))
        db.flush()
        items = list(db.scan(b"user:"))
        assert len(items) == N - N // 10
        for key, value in items[:500]:
            i = int(key.split(b":")[1])
            if i % 10 == 0:
                assert value == b"new-" + _val(i)
            else:
                assert value == _val(i)
        db.close()

        # persistence across restart
        db = DB(path, memtable_threshold=512 * 1024)
        assert db.get(_key(0)) == b"new-" + _val(0)
        assert db.get(_key(5)) is None
        assert db.get(_key(7)) == _val(7)
        assert len(list(db.scan(b"user:"))) == N - N // 10
        db.close()
