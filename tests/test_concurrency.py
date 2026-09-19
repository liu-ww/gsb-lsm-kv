"""Concurrency stress: 4 writer threads + 8 reader threads.

Writers own disjoint key ranges and keep a local expected-state model, so
at the end we can verify that no acknowledged write was ever lost.  Readers
hammer get()/scan() the whole time and validate everything they see.
Duration defaults to ~4s; set LSMKV_STRESS_SECS=30 for the full soak.
"""

import os
import random
import tempfile
import threading
import time

from lsmkv import DB

WRITERS = 4
READERS = 8
DURATION = float(os.environ.get("LSMKV_STRESS_SECS", "4"))


def test_concurrent_stress():
    with tempfile.TemporaryDirectory() as path:
        db = DB(path, memtable_threshold=256 * 1024)
        stop = threading.Event()
        errors = []
        models = [dict() for _ in range(WRITERS)]  # per-writer expected state
        counters = [0] * WRITERS

        def record_error(exc):
            errors.append(exc)
            stop.set()

        def writer(wid: int) -> None:
            rng = random.Random(wid)
            try:
                while not stop.is_set():
                    seq = counters[wid]
                    counters[wid] += 1
                    key = b"w%d:%08d" % (wid, seq)
                    value = b"w%d-v%d-" % (wid, seq) + bytes(rng.randbytes(20))
                    op = rng.random()
                    if op < 0.7 or not models[wid]:
                        db.put(key, value)
                        models[wid][key] = value
                    elif op < 0.9:
                        # overwrite a random live key of this writer
                        old_key = rng.choice(list(models[wid]))
                        db.put(old_key, value)
                        models[wid][old_key] = value
                    else:
                        victim = rng.choice(list(models[wid]))
                        db.delete(victim)
                        del models[wid][victim]
            except Exception as exc:  # noqa: BLE001
                record_error(exc)

        def reader(rid: int) -> None:
            rng = random.Random(1000 + rid)
            try:
                while not stop.is_set():
                    wid = rng.randrange(WRITERS)
                    seq = rng.randrange(max(counters[wid], 1))
                    key = b"w%d:%08d" % (wid, seq)
                    value = db.get(key)
                    if value is not None:
                        # any value we read must be well-formed and belong
                        # to the key's owning writer
                        assert value.startswith(b"w%d-v" % wid), (key, value)
                        tail = value.split(b"-", 2)[2]
                        assert len(tail) == 20
                    if rng.random() < 0.05:
                        prev = None
                        for k, _ in db.scan(b"w%d:" % wid):
                            if prev is not None:
                                assert k > prev, "scan out of order"
                            prev = k
            except Exception as exc:  # noqa: BLE001
                record_error(exc)

        threads = [
            threading.Thread(target=writer, args=(w,)) for w in range(WRITERS)
        ] + [
            threading.Thread(target=reader, args=(r,)) for r in range(READERS)
        ]
        for t in threads:
            t.start()
        time.sleep(DURATION)
        stop.set()
        for t in threads:
            t.join(timeout=60)

        assert not errors, errors

        # no acknowledged write was lost: final state matches the model
        total = 0
        for model in models:
            for key, value in model.items():
                got = db.get(key)
                assert got == value, f"{key}: expected {value!r}, got {got!r}"
                total += 1
        assert total > 0
        db.close()

        # and it all survives a restart
        db = DB(path, memtable_threshold=256 * 1024)
        for model in models:
            for key, value in model.items():
                assert db.get(key) == value
        db.close()
