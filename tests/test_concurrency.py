"""Concurrent readers/writers: no deadlocks, no lost committed writes, no torn reads."""

from __future__ import annotations

import random
import threading
import time

from lsmkv import DB


def _run(tmp_path, duration: float, sync: bool):
    db = DB(tmp_path, memtable_bytes=64 * 1024, level0_size=4, sync_on_commit=sync)
    stop = threading.Event()
    errors: list[Exception] = []
    committed: dict[bytes, bytes | None] = {}
    lock = threading.Lock()

    def writer(wid: int) -> None:
        rng = random.Random(wid)
        i = 0
        while not stop.is_set():
            key = f"w{wid}:k{rng.randrange(1500):06d}".encode()
            try:
                if rng.random() < 0.05:
                    db.delete(key)
                    with lock:
                        committed[key] = None
                else:
                    value = f"{wid}:{i}:".encode() + b"x" * rng.randrange(0, 400)
                    db.put(key, value)
                    with lock:
                        committed[key] = value
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
                return
            i += 1

    def reader(rid: int) -> None:
        rng = random.Random(1000 + rid)
        while not stop.is_set():
            key = f"w{rng.randrange(4)}:k{rng.randrange(1500):06d}".encode()
            value = db.get(key)
            if value is not None:
                try:
                    head, rest = value.split(b":", 1)
                    mid, tail = rest.split(b":", 1)
                    int(head)
                    int(mid)
                    assert set(tail) <= {ord("x")}
                except Exception as exc:
                    errors.append(exc)
                    return
            if rng.random() < 0.005:
                prev = b""
                for k, _ in db.scan(b"w0:"):
                    if k < prev:
                        errors.append(AssertionError("scan out of order"))
                        return
                    prev = k

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    threads += [threading.Thread(target=reader, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    time.sleep(duration)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors[:3]
    assert all(not t.is_alive() for t in threads), "deadlock: thread still alive"

    with lock:
        snapshot = dict(committed)
    for key, expected in snapshot.items():
        got = db.get(key)
        if expected is None:
            assert got is None, key
        else:
            assert got == expected, key
    db.close()

    db = DB(tmp_path)
    for key, expected in snapshot.items():
        got = db.get(key)
        if expected is None:
            assert got is None
        else:
            assert got == expected
    db.close()


def test_concurrent_fsync_off(tmp_path):
    _run(tmp_path, duration=12, sync=False)


def test_concurrent_fsync_on(tmp_path):
    _run(tmp_path, duration=6, sync=True)
