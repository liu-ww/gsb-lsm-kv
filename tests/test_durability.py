"""WAL torn-write recovery and subprocess crash recovery."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

from lsmkv import DB
from lsmkv.wal import encode_record
from lsmkv.types import Entry


def _crashed_db(base: Path, n: int = 2000) -> Path:
    """Spawn a child that writes with fsync, overwrites + deletes, then os._exit(1)."""
    work = base / "crashdb"
    work.mkdir(parents=True)
    script = textwrap.dedent(
        f"""
        import os, sys
        sys.path.insert(0, {os.getcwd()!r})
        from lsmkv import DB
        db = DB({str(work)!r}, sync_on_commit=True)
        N = {n}
        expected = {{}}
        for i in range(N):
            k = f"k{{i:06d}}".encode()
            v = f"initial-{{i}}".encode()
            db.put(k, v)
            expected[k] = v
        # overwrites
        for i in range(0, N, 3):
            k = f"k{{i:06d}}".encode()
            v = f"overwrite-{{i}}".encode()
            db.put(k, v)
            expected[k] = v
        # deletes
        deleted = []
        for i in range(0, N, 7):
            k = f"k{{i:06d}}".encode()
            db.delete(k)
            expected.pop(k, None)
            deleted.append(k)
        # one final big value committed right before the crash
        db.put(b"__final__", b"z" * 4096)
        expected[b"__final__"] = b"z" * 4096
        # hard crash: skip every interpreter finalizer / flush
        os._exit(1)
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=os.getcwd(),
        capture_output=True,
        timeout=120,
    )
    assert proc.returncode == 1, proc.stderr.decode(errors="replace")
    return work


def test_crash_recovery_subprocess(tmp_path):
    work = _crashed_db(tmp_path)
    n = 2000
    db = DB(work, sync_on_commit=True)
    missing = []
    for i in range(n):
        k = f"k{i:06d}".encode()
        got = db.get(k)
        if i % 7 == 0:
            if got is not None:
                missing.append(("deleted visible", k, got))
        elif i % 3 == 0:
            if got != f"overwrite-{i}".encode():
                missing.append(("overwrite", k, got))
        else:
            if got != f"initial-{i}".encode():
                missing.append(("initial", k, got))
    assert db.get(b"__final__") == b"z" * 4096
    # scan must exactly match the set of live keys, in order
    scanned = list(db.scan(b"k"))
    expected_live = [f"k{i:06d}".encode() for i in range(n) if i % 7 != 0]
    assert [k for k, _ in scanned] == expected_live
    assert not missing, missing[:5]
    db.close()

    # Second reopen: stable after a clean close too.
    db = DB(work)
    assert db.get(b"__final__") == b"z" * 4096
    db.close()


def test_wal_torn_tail_is_truncated(tmp_path):
    work = tmp_path / "db"
    db = DB(work, sync_on_commit=True)
    for i in range(100):
        db.put(f"k{i:04d}".encode(), f"v{i}".encode())
    db.delete(b"k0050")
    # Simulate kill -9: do not close(); the WAL stays unflushed.
    wal = next(work.glob("wal-*.log"))
    raw = wal.read_bytes()
    good_keys = [(f"k{i:04d}".encode(), None if i == 50 else f"v{i}".encode()) for i in range(100)]

    extra = encode_record(Entry(b"k0100", b"LOST", 101, False))
    for idx, cut in enumerate((1, len(extra) // 2, len(extra) - 1)):
        work_copy = tmp_path / f"db_torn_{idx}"
        shutil.copytree(work, work_copy)
        wal_copy = next(work_copy.glob("wal-*.log"))
        wal_copy.write_bytes(raw + extra[:cut])
        reopened = DB(work_copy, sync_on_commit=True)
        for k, v in good_keys:
            assert reopened.get(k) == v
        assert reopened.get(b"k0100") is None
        assert wal_copy.read_bytes() == raw
        reopened.close()

    # Chopping 5 bytes off the final committed frame (the k0050 tombstone,
    # which is 30 bytes long) makes that whole frame torn: it is dropped, so
    # k0050 reverts to its earlier live value v50.  Every earlier frame
    # survives intact.
    work2 = tmp_path / "db_chop"
    shutil.copytree(work, work2)
    wal2 = next(work2.glob("wal-*.log"))
    wal2.write_bytes(raw[:-5])
    reopened = DB(work2)
    assert reopened.get(b"k0050") == b"v50"
    assert reopened.get(b"k0099") == b"v99"
    assert reopened.get(b"k0049") == b"v49"
    reopened.close()

    # Chopping off the whole tombstone frame plus a little of the previous one
    # loses both frames: k0099 is gone too, but k0098 still reads correctly.
    work2b = tmp_path / "db_chop2"
    shutil.copytree(work, work2b)
    wal2b = next(work2b.glob("wal-*.log"))
    wal2b.write_bytes(raw[: -30 - 4])
    reopened = DB(work2b)
    assert reopened.get(b"k0050") == b"v50"
    assert reopened.get(b"k0099") is None
    assert reopened.get(b"k0098") == b"v98"
    reopened.close()


def test_wal_committed_corruption_raises(tmp_path):
    import pytest

    from lsmkv.wal import WalCorruptError

    work = tmp_path / "db"
    db = DB(work, sync_on_commit=True)
    for i in range(100):
        db.put(f"k{i:04d}".encode(), f"v{i}".encode())
    work3 = tmp_path / "db_corrupt"
    shutil.copytree(work, work3)
    wal3 = next(work3.glob("wal-*.log"))
    raw = bytearray(wal3.read_bytes())
    raw[40] ^= 0xFF
    wal3.write_bytes(bytes(raw))
    with pytest.raises(WalCorruptError):
        DB(work3)


def test_reopen_roundtrip_with_flushes(tmp_path):
    work = tmp_path / "db"
    db = DB(work, memtable_bytes=8192)
    for i in range(3000):
        db.put(f"k{i:06d}".encode(), f"value-{i}".encode())
    if i % 2 == 0:
        pass
    db.delete(b"k000004")
    db.close()
    db = DB(work, memtable_bytes=8192)
    assert db.get(b"k000000") == b"value-0"
    assert db.get(b"k000004") is None
    assert db.get(b"k002999") == b"value-2999"
    assert len(list(db.scan(b"k"))) == 2999
    db.close()
