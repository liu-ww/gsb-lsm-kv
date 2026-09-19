"""Crash recovery: a child process writes data (with overwrites and
deletes), then dies via os._exit(1).  The parent reopens the same DB and
verifies every acknowledged write matches the expected final state."""

import os
import subprocess
import sys
import tempfile

from lsmkv import DB
from tests.crash_child import expected_state

N = 3000


def test_crash_recovery():
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with tempfile.TemporaryDirectory() as path:
        env = dict(os.environ)
        env["LSMKV_REPO"] = repo
        child = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_child.py")
        proc = subprocess.run(
            [sys.executable, child, path, str(N)],
            env=env,
            capture_output=True,
            timeout=300,
        )
        assert proc.returncode == 1, proc.stderr.decode()

        expected = expected_state(N)
        db = DB(path)
        # every surviving key has exactly its final value
        for key, value in expected.items():
            got = db.get(key)
            assert got == value, f"{key}: expected {value!r}, got {got!r}"
        # every deleted key stays deleted
        for i in range(0, N, 3):
            assert db.get(b"key%06d" % i) is None
        # scan sees exactly the expected set
        scanned = dict(db.scan(b"key"))
        assert scanned == expected
        db.close()
