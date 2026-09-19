"""Stdlib-only fallback test runner (pytest-compatible tests live alongside).

Usage:  python tests/run_tests.py [test_module_name ...]
"""

import importlib
import inspect
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MODULES = [
    "tests.test_basic",
    "tests.test_recovery",
    "tests.test_flush_scan",
    "tests.test_crash",
    "tests.test_concurrency",
]


def main() -> int:
    modules = sys.argv[1:] or MODULES
    modules = [m if m.startswith("tests.") else f"tests.{m}" for m in modules]
    failures = 0
    for mod_name in modules:
        mod = importlib.import_module(mod_name)
        for name, fn in sorted(vars(mod).items()):
            if not (name.startswith("test_") and callable(fn)):
                continue
            start = time.perf_counter()
            try:
                fn()
                print(f"PASS {mod_name}.{name} ({time.perf_counter()-start:.1f}s)")
            except Exception:
                failures += 1
                print(f"FAIL {mod_name}.{name}")
                traceback.print_exc()
    print("=" * 40)
    print("FAILURES:", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
