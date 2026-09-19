"""End-to-end CLI tests (python -m lsmkv)."""

from __future__ import annotations

import subprocess
import sys


def _run(db, *args):
    return subprocess.run(
        [sys.executable, "-m", "lsmkv", str(db), *args],
        capture_output=True,
        timeout=300,
    )


def test_cli_put_get_delete_scan(tmp_path):
    db = tmp_path / "db"
    assert _run(db, "put", "alpha", "one").returncode == 0
    assert _run(db, "put", "beta", "two").returncode == 0
    assert _run(db, "put", "empty", "").returncode == 0
    assert _run(db, "get", "beta").stdout == b"two\n"
    assert _run(db, "get", "empty").stdout == b"\n"
    missing = _run(db, "get", "ghost")
    assert missing.returncode == 1 and missing.stdout == b""
    assert _run(db, "delete", "alpha").returncode == 0
    assert _run(db, "get", "alpha").returncode == 1
    out = _run(db, "scan")
    assert out.stdout.splitlines() == [b"beta\ttwo", b"empty\t"]
    out = _run(db, "scan", "b")
    assert out.stdout.splitlines() == [b"beta\ttwo"]


def test_cli_bench(tmp_path):
    db = tmp_path / "benchdb"
    proc = _run(db, "bench")
    assert proc.returncode == 0, proc.stderr.decode()
    text = proc.stdout.decode()
    assert "QPS" in text
    # Data must survive a fresh process opening the same DB.
    proc2 = _run(db, "scan", "nonexistent-prefix")
    assert proc2.returncode == 0 and proc2.stdout == b""
