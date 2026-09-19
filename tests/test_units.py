"""Unit tests for the hand-written skip list and framing primitives."""

from __future__ import annotations

import os

from lsmkv.skiplist import SkipList
from lsmkv.types import Entry
from lsmkv.wal import HEADER_SIZE, MAGIC, encode_record, replay
from lsmkv.sstable import KEYS_PER_BLOCK, SSTWriter, SSTable


def test_skiplist_ordering_and_overwrite():
    sl = SkipList()
    words = [b"banana", b"apple", b"cherry", b"date", b"apple", b"avocado"]
    for i, w in enumerate(words):
        sl.put(Entry(w, str(i).encode(), i, False))
    keys = [e.key for e in sl.entries()]
    assert keys == [b"apple", b"avocado", b"banana", b"cherry", b"date"]
    assert sl.get(b"apple").value == b"4"
    assert sl.get(b"missing") is None
    assert len(sl) == 5


def test_skiplist_tombstone_roundtrip():
    sl = SkipList()
    sl.put(Entry(b"k", b"v", 1, False))
    sl.put(Entry(b"k", b"", 2, True))
    e = sl.get(b"k")
    assert e.is_tombstone and e.value == b""


def test_wal_record_frame_layout(tmp_path):
    p = tmp_path / "w.log"
    entries = [
        Entry(b"a", b"123", 1, False),
        Entry(b"b", b"", 2, True),
        Entry(b"\x00\xff", b"\x00" * 10, 3, False),
    ]
    with open(p, "wb") as f:
        for e in entries:
            f.write(encode_record(e))
    replayed, torn, valid = replay(p)
    assert torn is False
    assert valid == p.stat().st_size
    assert replayed == entries
    # first frame starts with WAL magic
    assert int.from_bytes(p.read_bytes()[:4], "little") == MAGIC


def test_sstable_sparse_index_density(tmp_path):
    w = SSTWriter(0, 1, tmp_path)
    n = KEYS_PER_BLOCK * 3 + 5
    for i in range(n):
        w.add(Entry(f"k{i:06d}".encode(), b"v", i, i % 100 == 0))
    t = SSTable(w.finish())
    assert len(t._index_items) == 4  # one sparse point per 64 keys
    assert [it[0] for it in t._index_items] == [b"k000000", b"k000064", b"k000128", b"k000192"]
    for i in range(n):
        e = t.get(f"k{i:06d}".encode())
        assert e is not None and e.is_tombstone == (i % 100 == 0)


def test_prefix_upper_bound_semantics():
    from lsmkv.db import _prefix_upper_bound

    assert _prefix_upper_bound(b"") > b"\xff" * 1000
    assert _prefix_upper_bound(b"a") == b"b"
    assert _prefix_upper_bound(b"k\xff") == b"l"
    assert _prefix_upper_bound(b"ab\xff\xff") == b"ac"
    assert _prefix_upper_bound(b"\xff\xff") > b"\xff" * 1000


def test_scan_with_all_ff_keys(tmp_path):
    from lsmkv import DB

    db = DB(tmp_path, memtable_bytes=4096)
    db.put(b"\xff", b"a")
    db.put(b"\xff\xff", b"b")
    db.put(b"\xff\x00", b"c")
    db.put(b"normal", b"d")
    all_items = list(db.scan(b""))
    assert [k for k, _ in all_items] == [b"normal", b"\xff", b"\xff\x00", b"\xff\xff"]
    ff_items = list(db.scan(b"\xff"))
    assert {k for k, _ in ff_items} == {b"\xff", b"\xff\xff", b"\xff\x00"}
    db.close()
