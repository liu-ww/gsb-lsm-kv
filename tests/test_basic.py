"""Basic read/write/delete semantics."""

import os
import tempfile

from lsmkv import DB


def test_put_get_delete():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        db.put(b"foo", b"bar")
        assert db.get(b"foo") == b"bar"
        assert db.get(b"missing") is None
        db.delete(b"foo")
        assert db.get(b"foo") is None
        db.close()


def test_overwrite_returns_latest():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(100):
            db.put(b"key", b"value-%d" % i)
        assert db.get(b"key") == b"value-99"
        db.close()


def test_delete_then_rewrite():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        db.put(b"k", b"v1")
        db.delete(b"k")
        assert db.get(b"k") is None
        db.put(b"k", b"v2")
        assert db.get(b"k") == b"v2"
        db.flush()  # make sure tombstone+rewrite survive a flush too
        assert db.get(b"k") == b"v2"
        db.close()


def test_empty_value_is_distinct_from_missing():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        db.put(b"empty", b"")
        assert db.get(b"empty") == b""
        assert db.get(b"empty") is not None
        assert db.get(b"never-written") is None
        db.delete(b"empty")
        assert db.get(b"empty") is None
        db.close()


def test_value_sizes():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        v1k = os.urandom(1024)
        v100k = os.urandom(100 * 1024)
        db.put(b"v0", b"")
        db.put(b"v1k", v1k)
        db.put(b"v100k", v100k)
        assert db.get(b"v0") == b""
        assert db.get(b"v1k") == v1k
        assert db.get(b"v100k") == v100k
        db.flush()
        assert db.get(b"v1k") == v1k
        assert db.get(b"v100k") == v100k
        db.close()
        # and after reopen
        db = DB(path)
        assert db.get(b"v100k") == v100k
        db.close()


def test_binary_safety():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        keys = [bytes([i]) * 3 for i in range(256)]
        for i, k in enumerate(keys):
            db.put(k, bytes([255 - i]) * 10)
        for i, k in enumerate(keys):
            assert db.get(k) == bytes([255 - i]) * 10
        db.put(b"\x00\x00", b"\x00")
        assert db.get(b"\x00\x00") == b"\x00"
        db.close()


def test_scan_prefix_ordered_and_filters_tombstones():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        db.put(b"user:1", b"alice")
        db.put(b"user:2", b"bob")
        db.put(b"user:3", b"carol")
        db.put(b"order:1", b"x")
        db.delete(b"user:2")
        items = list(db.scan(b"user:"))
        assert items == [(b"user:1", b"alice"), (b"user:3", b"carol")]
        # full scan is globally ordered
        all_items = list(db.scan(b""))
        assert [k for k, _ in all_items] == sorted(k for k, _ in all_items)
        assert len(all_items) == 3
        db.close()


def test_scan_is_lazy_iterator():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(100):
            db.put(b"k%03d" % i, b"v")
        it = db.scan(b"k")
        assert not isinstance(it, list)
        first = next(it)
        assert first == (b"k000", b"v")
        rest = list(it)
        assert len(rest) == 99
        db.close()


def test_str_keys_accepted():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        db.put("hello", "world")
        assert db.get("hello") == b"world"
        db.close()
