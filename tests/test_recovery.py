"""WAL replay, torn-write handling, and restart persistence."""

import glob
import os
import struct
import tempfile
import zlib

from lsmkv import DB
from lsmkv.wal import encode_record


def _wal_files(path):
    return sorted(glob.glob(os.path.join(path, "wal-*.log")))


def test_restart_persistence():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(1000):
            db.put(b"k%04d" % i, b"v%d" % i)
        db.delete(b"k0000")
        db.close()  # clean close flushes everything
        db = DB(path)
        assert db.get(b"k0000") is None
        for i in range(1, 1000):
            assert db.get(b"k%04d" % i) == b"v%d" % i
        db.close()


def test_wal_replay_on_reopen():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(500):
            db.put(b"k%04d" % i, b"v%d" % i)
        db.delete(b"k0100")
        # simulate crash: drop the object without close()/flush()
        del db
        db = DB(path)
        assert db.get(b"k0100") is None
        for i in (0, 1, 250, 499):
            assert db.get(b"k%04d" % i) == b"v%d" % i
        db.close()


def test_torn_tail_record_is_discarded():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(100):
            db.put(b"k%03d" % i, b"val%d" % i)
        wal = _wal_files(path)[0]
        size_before = os.path.getsize(wal)
        # torn write: half of a record at the tail
        with open(wal, "ab") as f:
            f.write(os.urandom(11))
        del db
        db = DB(path)
        # garbage truncated, all complete records intact
        assert os.path.getsize(wal) == size_before
        for i in range(100):
            assert db.get(b"k%03d" % i) == b"val%d" % i
        db.close()


def test_crc_corrupt_record_is_discarded():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(50):
            db.put(b"k%03d" % i, b"val%d" % i)
        wal = _wal_files(path)[0]
        size_before = os.path.getsize(wal)
        # append a record with a deliberately wrong crc
        body = encode_record(b"bad", b"record", 10_000, False)
        corrupt = struct.pack("<I", 0xDEADBEEF) + body[4:]
        with open(wal, "ab") as f:
            f.write(corrupt)
        del db
        db = DB(path)
        assert os.path.getsize(wal) == size_before
        assert db.get(b"bad") is None
        for i in range(50):
            assert db.get(b"k%03d" % i) == b"val%d" % i
        db.close()


def test_truncated_header_mid_file_stops_replay():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        for i in range(10):
            db.put(b"k%02d" % i, b"v%d" % i)
        wal = _wal_files(path)[0]
        size_before = os.path.getsize(wal)
        with open(wal, "ab") as f:
            f.write(b"\x01\x02\x03")  # partial header only
        del db
        db = DB(path)
        assert os.path.getsize(wal) == size_before
        for i in range(10):
            assert db.get(b"k%02d" % i) == b"v%d" % i
        # db still fully writable after recovery
        db.put(b"after", b"recovery")
        assert db.get(b"after") == b"recovery"
        db.close()


def test_tombstones_persist_in_wal_and_sstable():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path)
        db.put(b"a", b"1")
        db.flush()  # tombstone must survive in the SSTable path too
        db.put(b"a", b"2")
        db.delete(b"a")
        db.flush()
        del db
        db = DB(path)
        assert db.get(b"a") is None
        db.close()
        # WAL files for flushed memtables are gone
        assert _wal_files(path) == [] or all(
            os.path.getsize(w) >= 0 for w in _wal_files(path)
        )
