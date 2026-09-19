#!/usr/bin/env python3
"""Stdlib-only test runner.

Tests in tests/ use plain pytest conventions: functions named ``test_*`` with
an optional ``tmp_path`` fixture, and ``pytest.raises``.  With pytest
installed, prefer ``pytest``; this file is a zero-dependency fallback that
runs the exact same modules with only the standard library.
"""

from __future__ import annotations

import importlib
import inspect
import importlib.util
import pathlib
import shutil
import sys
import tempfile
import time
import traceback
import types

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


class _Raises:
    def __init__(self, exc_type):
        self.exc_type = exc_type
        self.value = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f"{self.exc_type.__name__} was not raised")
        if not issubclass(exc_type, self.exc_type):
            return False
        self.value = exc
        return True


def _install_pytest_shim() -> None:
    pytest = types.ModuleType("pytest")
    pytest.raises = _Raises

    class _Mark:
        def __getattr__(self, name):
            def deco(fn=None, *args, **kwargs):
                if fn is None:
                    return lambda f: f
                return fn
            return deco

    pytest.mark = _Mark()
    pytest.skip = lambda *a, **k: None
    sys.modules.setdefault("pytest", pytest)


def _load_module(path: pathlib.Path):
    name = "tests_" + path.stem
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def main() -> int:
    _install_pytest_shim()
    test_files = sorted((ROOT / "tests").glob("test_*.py"))
    total = passed = 0
    failures: list[tuple[str, str]] = []
    started = time.perf_counter()
    for tf in test_files:
        module = _load_module(tf)
        for name, fn in sorted(vars(module).items()):
            if not name.startswith("test_") or not callable(fn):
                continue
            total += 1
            tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"{tf.stem}-{name}-"))
            try:
                params = inspect.signature(fn).parameters
                if "tmp_path" in params:
                    fn(tmp)
                else:
                    fn()
            except Exception:
                failures.append((f"{tf.name}::{name}", traceback.format_exc()))
                print("F", end="", flush=True)
            else:
                passed += 1
                print(".", end="", flush=True)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
    dt = time.perf_counter() - started
    print()
    print(f"{passed}/{total} passed in {dt:.1f}s")
    for name, tb in failures:
        print("=" * 70)
        print("FAIL", name)
        print(tb)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
