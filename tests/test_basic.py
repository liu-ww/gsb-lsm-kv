"""Basic API behaviour: put/get/delete, overwrite, empty values, binary data."""

import os

from lsmkv import DB
from lsmkv.types import TOMBSTONE


def _db(tmp, **kw):
    return DB(str(tmp), **kw)


def test_put_get(tmp_path):
    db = _db(tmp_path)
    assert db.get(b"missing") is None
    db.put(b"a", b"1")
    assert db.get(b"a") == b"1"
    db.close()


def test_empty_string_distinct_from_missing(tmp_path):
    db = _db(tmp_path)
    db.put(b"e", b"")
    assert db.get(b"e") == b""
    assert db.get(b"missing") is None
    db.close()


def test_overwrite_takes_newest(tmp_path):
    db = _db(tmp_path, memtable_bytes=4096)
    for i in range(300):
        db.put(b"k", str(i).encode())
    assert db.get(b"k") == b"299"
    db.close()


def test_delete_is_invisible(tmp_path):
    db = _db(tmp_path, memtable_bytes=4096)
    db.put(b"k", b"v")
    db.delete(b"k")
    assert db.get(b"k") is None
    db.close()


def test_delete_then_rewrite(tmp_path):
    db = _db(tmp_path, memtable_bytes=4096)
    db.put(b"k", b"v1")
    for i in range(500):
        db.put(f"f{i}".encode(), b"x")
    db.delete(b"k")
    assert db.get(b"k") is None
    db.put(b"k", b"v2")
    assert db.get(b"k") == b"v2"
    db.compact_now()
    assert db.get(b"k") == b"v2"
    db.close()
    db = _db(tmp_path, memtable_bytes=4096)
    assert db.get(b"k") == b"v2"
    db.close()


def test_large_values_1kb_and_100kb(tmp_path):
    db = _db(tmp_path, memtable_bytes=64 * 1024)
    db.put(b"small", b"x" * 1024)
    big = os.urandom(100 * 1024)
    db.put(b"big", big)
    db.put(b"empty", b"")
    assert db.get(b"small") == b"x" * 1024
    assert db.get(b"big") == big
    assert db.get(b"empty") == b""
    db.compact_now()
    assert db.get(b"big") == big
    db.close()
    db = _db(tmp_path, memtable_bytes=64 * 1024)
    assert db.get(b"big") == big
    db.close()


def test_binary_safe_keys_and_values(tmp_path):
    db = _db(tmp_path, memtable_bytes=4096)
    pairs = []
    for i in range(20):
        key = bytes(range(256)) + bytes([i])
        value = os.urandom(777) + b"\x00\xff" * 50
        pairs.append((key, value))
    for k, v in pairs:
        db.put(k, v)
    for k, v in pairs:
        assert db.get(k) == v
    db.close()


def test_prefix_scan_filters_and_orders(tmp_path):
    db = _db(tmp_path, memtable_bytes=4096)
    for i in range(300):
        db.put(f"a:{i:04d}".encode(), b"v")
        db.put(f"b:{i:04d}".encode(), b"v")
    db.delete(b"a:0010")
    items = list(db.scan(b"a:"))
    assert len(items) == 299
    keys = [k for k, _ in items]
    assert keys == sorted(keys)
    assert all(k.startswith(b"a:") for k in keys)
    assert b"a:0010" not in keys
    assert len(list(db.scan(b"b:"))) == 300
    assert len(list(db.scan(b"nope:"))) == 0
    db.close()


def test_scan_is_lazy_and_snapshot(tmp_path):
    db = _db(tmp_path, memtable_bytes=4096)
    for i in range(300):
        db.put(f"k{i:04d}".encode(), b"v")
    it = db.scan(b"k")
    first = next(it)
    db.put(b"k9999", b"later")
    rest = list(it)
    assert first[0] == b"k0000"
    assert b"k9999" not in {k for k, _ in rest}
    db.close()


def test_type_errors(tmp_path):
    db = _db(tmp_path)
    import pytest

    with pytest.raises(TypeError):
        db.put("str", b"v")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        db.get("str")  # type: ignore[arg-type]
    db.close()
