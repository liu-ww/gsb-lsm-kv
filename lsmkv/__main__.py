"""Command line interface:  python -m lsmkv <db_path> <command> ...

Commands:
    put <key> <value>     store a key/value pair
    get <key>             print the value (raw bytes) to stdout
    delete <key>          delete a key (tombstone)
    scan <prefix>         print all key/value pairs with the prefix, in order
    bench [count]         write `count` random keys (~100B values), read them
                          all back, verify, and print write/read QPS
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from .engine import DB


def _cmd_put(db: DB, args: argparse.Namespace) -> int:
    db.put(args.key.encode("utf-8"), args.value.encode("utf-8"))
    return 0


def _cmd_get(db: DB, args: argparse.Namespace) -> int:
    value = db.get(args.key.encode("utf-8"))
    if value is None:
        print(f"key not found: {args.key}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(value + b"\n")
    sys.stdout.buffer.flush()
    return 0


def _cmd_delete(db: DB, args: argparse.Namespace) -> int:
    db.delete(args.key.encode("utf-8"))
    return 0


def _cmd_scan(db: DB, args: argparse.Namespace) -> int:
    out = sys.stdout.buffer
    for key, value in db.scan(args.prefix.encode("utf-8")):
        out.write(key + b" => " + value + b"\n")
    out.flush()
    return 0


def _cmd_bench(db: DB, args: argparse.Namespace) -> int:
    count: int = args.count
    value_size: int = args.value_size
    rng = random.Random(20240913)
    keys = [rng.randbytes(16) for _ in range(count)]
    values = [rng.randbytes(value_size) for _ in range(count)]

    print(f"bench: writing {count} keys ({value_size}B values) ...")
    start = time.perf_counter()
    for key, value in zip(keys, values):
        db.put(key, value)
    write_secs = time.perf_counter() - start
    write_qps = count / write_secs if write_secs > 0 else float("inf")
    print(f"write: {count} ops in {write_secs:.2f}s -> {write_qps:,.0f} QPS")

    print("bench: reading back and verifying ...")
    start = time.perf_counter()
    for key, expected in zip(keys, values):
        got = db.get(key)
        if got != expected:
            print(f"VERIFY FAILED for key {key.hex()}", file=sys.stderr)
            return 1
    read_secs = time.perf_counter() - start
    read_qps = count / read_secs if read_secs > 0 else float("inf")
    print(f"read:  {count} ops in {read_secs:.2f}s -> {read_qps:,.0f} QPS")
    print("bench: all values verified OK")
    return 0


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(prog="lsmkv", description=__doc__)
    parser.add_argument("db_path", help="path to the database directory")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("put", help="store a key/value pair")
    p.add_argument("key")
    p.add_argument("value")
    p.set_defaults(func=_cmd_put)

    p = sub.add_parser("get", help="print the value for a key")
    p.add_argument("key")
    p.set_defaults(func=_cmd_get)

    p = sub.add_parser("delete", help="delete a key")
    p.add_argument("key")
    p.set_defaults(func=_cmd_delete)

    p = sub.add_parser("scan", help="print pairs with a key prefix, in order")
    p.add_argument("prefix")
    p.set_defaults(func=_cmd_scan)

    p = sub.add_parser("bench", help="write/read benchmark with verification")
    p.add_argument("count", nargs="?", type=int, default=100_000)
    p.add_argument("--value-size", type=int, default=100)
    p.set_defaults(func=_cmd_bench)

    args = parser.parse_args(argv)
    with DB(args.db_path) as db:
        return args.func(db, args)


if __name__ == "__main__":
    raise SystemExit(main())
