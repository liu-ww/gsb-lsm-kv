"""Command line interface: python -m lsmkv <db_path> <command> ..."""

from __future__ import annotations

import os
import random
import sys
import time

from .db import DB

BENCH_COUNT = 100_000
BENCH_VALUE_SIZE = 100


def _out(data: bytes) -> None:
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) < 2:
        print(
            "usage: python -m lsmkv <db_path> {put|get|delete|scan|bench} ...",
            file=sys.stderr,
        )
        return 2
    db_path, command = args[0], args[1]
    rest = args[2:]

    if command == "bench":
        return _bench(db_path)

    with DB(db_path) as db:
        if command == "put":
            if len(rest) != 2:
                print("usage: put <key> <value>", file=sys.stderr)
                return 2
            db.put(os.fsencode(rest[0]), os.fsencode(rest[1]))
        elif command == "get":
            if len(rest) != 1:
                print("usage: get <key>", file=sys.stderr)
                return 2
            value = db.get(os.fsencode(rest[0]))
            if value is None:
                return 1
            _out(value)
        elif command == "delete":
            if len(rest) != 1:
                print("usage: delete <key>", file=sys.stderr)
                return 2
            db.delete(os.fsencode(rest[0]))
        elif command == "scan":
            if len(rest) > 1:
                print("usage: scan [prefix]", file=sys.stderr)
                return 2
            prefix = os.fsencode(rest[0]) if rest else b""
            for key, value in db.scan(prefix):
                sys.stdout.buffer.write(key + b"\t" + value + b"\n")
            sys.stdout.buffer.flush()
        else:
            print(f"unknown command: {command}", file=sys.stderr)
            return 2
    return 0


def _bench(db_path: str) -> int:
    rng = random.Random(20260919)
    # Fresh DB for a deterministic run.
    import shutil

    shutil.rmtree(db_path, ignore_errors=True)
    pad = b"v" * BENCH_VALUE_SIZE

    with DB(db_path, sync_on_commit=True) as db:
        keys: list[bytes] = []
        t0 = time.perf_counter()
        for i in range(BENCH_COUNT):
            key = f"bench:{rng.randrange(1 << 40):032x}:{i}".encode()
            value = (f"{i:08d}".encode() + pad)[:BENCH_VALUE_SIZE]
            db.put(key, value)
            keys.append(key)
        write_dt = time.perf_counter() - t0

        rng.shuffle(keys)
        t0 = time.perf_counter()
        for i, key in enumerate(keys):
            value = db.get(key)
            if value is None or len(value) != BENCH_VALUE_SIZE:
                print(f"verification failed for {key!r}: {value!r}", file=sys.stderr)
                return 1
        read_dt = time.perf_counter() - t0

        # Full prefix scan must return exactly the 100k keys, ordered & unique.
        seen = 0
        prev = b""
        for key, _value in db.scan(b"bench:"):
            if key < prev:
                print("scan ordering failure", file=sys.stderr)
                return 1
            prev = key
            seen += 1
        if seen != BENCH_COUNT:
            print(f"scan count failure: {seen} != {BENCH_COUNT}", file=sys.stderr)
            return 1
        counts = db.segment_counts()
        flushes = db.stats()["flushes"]

    print(f"writes : {BENCH_COUNT} in {write_dt:.3f}s  {BENCH_COUNT / write_dt:,.0f} QPS")
    print(f"reads  : {BENCH_COUNT} in {read_dt:.3f}s  {BENCH_COUNT / read_dt:,.0f} QPS")
    print(f"flushes: {flushes}  segments per level: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
