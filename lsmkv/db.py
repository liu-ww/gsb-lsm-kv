"""Embedded LSM-tree key/value engine.

Write path:
    put/delete -> append+fsync WAL record -> insert into the skip-list
    memtable -> at ~4MB freeze it, flush a key-ordered SSTable, publish via an
    atomically replaced MANIFEST, then delete the superseded WAL generation.

Read path:
    active memtable -> immutable frozen memtables -> segments newest-to-oldest
    (sparse index -> block binary search).  Reads take a snapshot of the
    segment lists, so background compaction never blocks them.

Compaction:
    one background thread, size-tiered: when a level holds >=
    ``level0_size`` segments it merges them (latest timestamp wins, tombstones
    dropped when no lower level can hide them) into the next level and
    atomically swaps the manifest.
"""

from __future__ import annotations

import heapq
import os
import queue
import threading
from pathlib import Path
from typing import Iterator

from . import manifest
from .skiplist import SkipList
from .sstable import SSTable, SSTWriter
from .types import Entry, TOMBSTONE
from .wal import WALWriter, replay

WAL_PREFIX = "wal-"
WAL_SUFFIX = ".log"
DEFAULT_MEMTABLE_BYTES = 4 * 1024 * 1024
DEFAULT_LEVEL0_SIZE = 4


def _wal_path(directory: Path, generation: int) -> Path:
    return directory / f"{WAL_PREFIX}{generation:08d}{WAL_SUFFIX}"


class FrozenMemtable:
    """An immutable skip list being flushed, still readable."""

    __slots__ = ("table", "wal_generation", "sequence", "table_id")

    def __init__(
        self,
        table: SkipList,
        wal_generation: int,
        sequence: int,
        table_id: int,
    ) -> None:
        self.table = table
        self.wal_generation = wal_generation
        self.sequence = sequence
        self.table_id = table_id


class DB:
    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        sync_on_commit: bool = True,
        memtable_bytes: int = DEFAULT_MEMTABLE_BYTES,
        level0_size: int = DEFAULT_LEVEL0_SIZE,
    ) -> None:
        self.directory = Path(path)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.sync = sync_on_commit
        self.memtable_bytes_threshold = memtable_bytes
        self.level0_size = level0_size
        self._max_frozen = 2

        self._rw = threading.Condition(threading.RLock())
        self._compaction_cv = threading.Condition()
        self._compact_lock = threading.Lock()
        self._manifest_lock = threading.Lock()
        self._closing = False
        self._closed = False

        self._levels: list[list[SSTable]] = []
        self._all_tables: set[SSTable] = set()
        self._memtable = SkipList()
        self._frozen: list[FrozenMemtable] = []
        self._sequence = 0
        self._next_id = 1
        self._wal_generation = 0
        self._wal: WALWriter | None = None

        self._stats = {"puts": 0, "deletes": 0, "flushes": 0, "compactions": 0}
        self._recover()
        # One dedicated flush thread guarantees frozen memtables become
        # segments in freeze order (parallel flush workers could publish
        # segments out of order and corrupt the newest-wins read chain).
        self._freeze_queue: "queue.Queue[FrozenMemtable | None]" = queue.Queue()
        self._flusher = threading.Thread(
            target=_flush_loop, args=(self,), name="lsmkv-flush", daemon=True
        )
        self._flusher.start()
        self._compactor = threading.Thread(
            target=_compaction_loop, args=(self,), name="lsmkv-compactor", daemon=True
        )
        self._compactor.start()

    # ------------------------------------------------------------------ open

    def _recover(self) -> None:
        with self._rw:
            seq, next_id, level_ids = manifest.read(self.directory)
            live_ids = {seg_id for ids in level_ids for seg_id in ids}
            # Remove segments that never made it into a committed manifest.
            for p in self.directory.glob("seg-*.sst"):
                try:
                    seg_id = int(p.name[len("seg-") : -len(".sst")])
                except ValueError:
                    seg_id = -1
                if seg_id not in live_ids:
                    p.unlink(missing_ok=True)
            for p in self.directory.glob("seg-*.tmp"):
                p.unlink(missing_ok=True)
            for p in self.directory.glob("MANIFEST.tmp"):
                p.unlink(missing_ok=True)

            self._next_id = next_id
            self._levels = []
            for ids in level_ids:
                tables: list[SSTable] = []
                for seg_id in ids:
                    table = SSTable(self.directory / f"seg-{seg_id:08d}.sst")
                    tables.append(table)
                    self._all_tables.add(table)
                self._levels.append(tables)
            self._sequence = seq

            # Replay every surviving WAL generation in numeric order.  Older
            # generations should normally have been removed after flushing;
            # they linger only when a crash happened mid-rotation.
            wal_files = sorted(self.directory.glob(f"{WAL_PREFIX}*{WAL_SUFFIX}"))
            recovered: list[Entry] = []
            for wp in wal_files:
                entries, _torn, valid_len = replay(wp)
                if _torn:
                    # Drop the torn tail on disk too, so later reopenings and
                    # WAL truncation accounting see a clean file.
                    with open(wp, "r+b") as f:
                        f.truncate(valid_len)
                        f.flush()
                        if self.sync:
                            os.fsync(f.fileno())
                recovered.extend(entries)
            if wal_files:
                self._wal_generation = max(
                    int(p.name[len(WAL_PREFIX) : -len(WAL_SUFFIX)]) for p in wal_files
                )
            if recovered or not wal_files:
                if not wal_files:
                    self._wal_generation = 1
                self._wal = WALWriter(_wal_path(self.directory, self._wal_generation), self.sync)
            else:
                self._wal_generation += 1
                self._wal = WALWriter(_wal_path(self.directory, self._wal_generation), self.sync)

            for entry in recovered:
                self._memtable.put(entry)
                if entry.sequence >= self._sequence:
                    self._sequence = entry.sequence + 1

    # ----------------------------------------------------------------- write

    def put(self, key: bytes, value: bytes) -> None:
        if not isinstance(key, (bytes, bytearray, memoryview)):
            raise TypeError("key must be bytes")
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("value must be bytes")
        self._write(bytes(key), bytes(value), False)

    def delete(self, key: bytes) -> None:
        if not isinstance(key, (bytes, bytearray, memoryview)):
            raise TypeError("key must be bytes")
        self._write(bytes(key), TOMBSTONE, True)

    def _write(self, key: bytes, value: bytes, is_tombstone: bool) -> None:
        with self._rw:
            if self._closed or self._closing:
                raise RuntimeError("database is closed")
            entry = Entry(key, value, self._sequence, is_tombstone)
            assert self._wal is not None
            self._wal.append(entry)  # fsync happens inside when sync is on
            self._memtable.put(entry)
            self._sequence += 1
            if is_tombstone:
                self._stats["deletes"] += 1
            else:
                self._stats["puts"] += 1
            self._freeze_if_needed_locked()

    def _freeze_if_needed_locked(self) -> None:
        """Called under the write lock.

        Backpressure: if too many frozen memtables are queued ahead of the
        flush thread, block writers so memory stays bounded.  Because all
        flushes are serialised in freeze order, published segments stay in the
        correct newest-first order.
        """
        while len(self._frozen) >= self._max_frozen:
            if self._closing:
                raise RuntimeError("database is closing")
            self._rw.wait()
        if self._memtable.estimate_bytes() < self.memtable_bytes_threshold:
            return
        if len(self._memtable) == 0:
            return
        old_generation = self._wal_generation
        self._wal_generation += 1
        new_wal = WALWriter(_wal_path(self.directory, self._wal_generation), self.sync)
        # Crash safety order: the new WAL must exist before we freeze; on
        # recovery both old and new WAL are replayed.
        self._wal = new_wal
        # Reserve the segment id now (under the write lock) and persist it via
        # MANIFEST, so a crash before the flush finishes cannot cause id reuse
        # on recovery.
        table_id = self._alloc_table_id_locked()
        frozen = FrozenMemtable(self._memtable, old_generation, self._sequence, table_id)
        self._frozen.append(frozen)
        self._memtable = SkipList()
        self._freeze_queue.put(frozen)
        self._persist_manifest_locked()

    def _publish_flush(self, frozen: FrozenMemtable, table: SSTable) -> None:
        with self._rw:
            self._all_tables.add(table)
            while len(self._levels) == 0:
                self._levels.append([])
            self._levels[0].insert(0, table)  # newest first
            self._persist_manifest_locked()
            # Segment + manifest are durable now; the WAL it covered is
            # redundant and can be deleted.
            wal_path = _wal_path(self.directory, frozen.wal_generation)
        # Unlink outside the lock; a crash here simply leaves a replayed-then-
        # overwritten WAL behind (harmless: same keys, older sequence).
        try:
            wal_path.unlink()
            self._fsync_dir()
        except FileNotFoundError:
            pass
        with self._rw:
            self._frozen.remove(frozen)
            self._stats["flushes"] += 1
            self._rw.notify_all()
        with self._compaction_cv:
            self._compaction_cv.notify()

    def _persist_manifest_locked(self) -> None:
        levels = [[t.table_id for t in level] for level in self._levels]
        # A flush and a compaction can publish concurrently; serialise the
        # temp-file + os.replace dance (they share one MANIFEST.tmp name).
        with self._manifest_lock:
            manifest.write(self.directory, self._sequence, self._next_id, levels, self.sync)

    def _alloc_table_id_locked(self) -> int:
        seg_id = self._next_id
        self._next_id += 1
        return seg_id

    def _fsync_dir(self) -> None:
        fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ------------------------------------------------------------------ read

    def _snapshot(self) -> tuple[SkipList, list[SkipList], list[list[SSTable]]]:
        with self._rw:
            return self._memtable, [f.table for f in self._frozen], [list(lv) for lv in self._levels]

    def get(self, key: bytes) -> bytes | None:
        """Return the latest live value, or None if the key does not exist.

        An existing key whose value is the empty byte string returns ``b""``,
        which is distinct from the ``None`` "not found" case.
        """
        if not isinstance(key, (bytes, bytearray, memoryview)):
            raise TypeError("key must be bytes")
        key = bytes(key)
        active, frozen, levels = self._snapshot()
        entry = active.get(key)
        if entry is not None:
            return None if entry.is_tombstone else entry.value
        for table in frozen:
            entry = table.get(key)
            if entry is not None:
                return None if entry.is_tombstone else entry.value
        for level in levels:
            for table in level:
                entry = table.get(key)
                if entry is not None:
                    return None if entry.is_tombstone else entry.value
        return None

    def scan(self, prefix: bytes = b"") -> Iterator[tuple[bytes, bytes]]:
        """Lazy, key-ordered iterator over live keys under ``prefix``.

        A snapshot of the segment lists is taken up front; the merge reads
        blocks lazily, so callers can stop iteration without scanning the whole
        key range.
        """
        if not isinstance(prefix, (bytes, bytearray, memoryview)):
            raise TypeError("prefix must be bytes")
        prefix = bytes(prefix)
        upper = _prefix_upper_bound(prefix)
        active, frozen, levels = self._snapshot()

        # Snapshot isolation: materialise the active memtable once so writes
        # that arrive after scan() started cannot appear in the iteration.
        # Frozen memtables and SSTables are already immutable.  Sources are
        # ordered newest -> oldest; each yields strictly increasing keys.
        active_entries = list(active.entries())
        sources: list[Iterator[Entry]] = [_bounded(iter(active_entries), prefix, upper)]
        for table in frozen:
            sources.append(_bounded(table.entries(), prefix, upper))
        for level in levels:
            for table in level:
                sources.append(_bounded(table.iter_from(prefix), prefix, upper))

        heap: list[tuple[bytes, int, Entry, Iterator[Entry]]] = []
        for rank, src in enumerate(sources):
            for first in src:
                heap.append((first.key, rank, first, src))
                break
        heapq.heapify(heap)

        while heap:
            key, _rank, entry, src = heapq.heappop(heap)
            # Every other source that still sits on this same key holds an
            # older version; consume it without yielding.
            while heap and heap[0][0] == key:
                _k, _r, _e, stale_src = heapq.heappop(heap)
                for nxt in stale_src:
                    if nxt.key < upper:
                        heapq.heappush(heap, (nxt.key, _r, nxt, stale_src))
                    break
            for nxt in src:
                if nxt.key < upper:
                    heapq.heappush(heap, (nxt.key, _rank, nxt, src))
                break
            if not entry.is_tombstone:
                yield key, entry.value

    # ------------------------------------------------------------- compaction

    def stats(self) -> dict[str, int]:
        with self._rw:
            return dict(self._stats)

    def segment_counts(self) -> list[int]:
        with self._rw:
            return [len(level) for level in self._levels]

    def compact_now(self) -> bool:
        """Run one size-tiered merge pass synchronously; True if it merged."""
        return _compact_once(self)

    def wait_for_compactions(self, timeout: float | None = None) -> None:
        """Block until the level-0 queue is below the trigger size."""
        import time

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._compaction_cv:
                with self._rw:
                    over = any(
                        len(level) >= self.level0_size for level in self._levels
                    )
                if not over:
                    return
                if deadline is not None and time.monotonic() >= deadline:
                    return
                self._compaction_cv.wait(0.02)

    def wait_for_flushes(self, timeout: float | None = 10.0) -> None:
        """Block until every frozen memtable has been published."""
        import time

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._rw:
            while self._frozen:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                if not self._rw.wait(timeout=0.01 if remaining is None else min(0.01, remaining)):
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError("memtable flush did not finish")

    # --------------------------------------------------------------- shutdown

    def close(self) -> None:
        with self._rw:
            if self._closed:
                return
            self._closing = True
            self._rw.notify_all()
            if len(self._memtable) > 0:
                # Clean shutdown: drain the residual memtable through the same
                # ordered flush queue.
                table_id = self._alloc_table_id_locked()
                frozen_mem = FrozenMemtable(
                    self._memtable, self._wal_generation, self._sequence, table_id
                )
                self._frozen.append(frozen_mem)
                self._memtable = SkipList()
                self._freeze_queue.put(frozen_mem)
        # Ordered shutdown: finish every queued flush (the residual memtable
        # was enqueued last), then stop the background threads.
        self._freeze_queue.put(None)
        self._flusher.join()
        with self._compaction_cv:
            self._closing = True
            self._compaction_cv.notify_all()
        self._compactor.join()
        # Belt and braces: make sure every frozen memtable got published even
        # if the flusher exited early.
        with self._rw:
            leftover = list(self._frozen)
        for frozen in leftover:
            _flush_one(self, frozen)
        with self._rw:
            for table in list(self._all_tables):
                table.close()
            self._all_tables.clear()
            if self._wal is not None:
                self._wal.close()
                self._wal = None
            self._closed = True
            self._rw.notify_all()

    def __enter__(self) -> "DB":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


# ------------------------------------------------------------------ helpers


def _bounded(it: Iterator[Entry], lower: bytes, upper: bytes) -> Iterator[Entry]:
    for entry in it:
        if entry.key < lower:
            continue
        if entry.key >= upper:
            return
        yield entry


def _prefix_upper_bound(prefix: bytes) -> bytes:
    """Smallest key strictly greater than every key starting with prefix.

    An empty prefix covers the whole key space; an all-0xFF prefix likewise
    has no successor inside finite byte strings.  Both return a sentinel
    larger than any real key we accept (we reject keys >= that sentinel in
    the scan filter, which never matches user data).
    """
    if not prefix:
        return _INFINITE_KEY
    data = bytearray(prefix)
    i = len(data) - 1
    while i >= 0 and data[i] == 0xFF:
        i -= 1
    if i < 0:
        return _INFINITE_KEY  # all-0xFF prefix: no finite successor
    data[i] += 1
    return bytes(data[: i + 1])  # trailing 0xFF bytes carry away


# A key longer than any we treat as user data for scan bounds.
_INFINITE_KEY = b"\xff" * (1 << 20)


def _flush_loop(db: "DB") -> None:
    while True:
        frozen = db._freeze_queue.get()
        if frozen is None:
            return
        try:
            _flush_one(db, frozen)
        except Exception:  # pragma: no cover - keep the loop alive
            import logging

            logging.getLogger("lsmkv").exception("memtable flush failed")


def _flush_one(db: "DB", frozen: FrozenMemtable) -> None:
    with db._rw:
        if db._closed:
            return
        seg_id = frozen.table_id
    writer = SSTWriter(0, seg_id, db.directory)
    try:
        for entry in frozen.table.entries():
            writer.add(entry)
        path = writer.finish()
    except BaseException:
        try:
            (db.directory / f"seg-{seg_id:08d}.tmp").unlink(missing_ok=True)
        except OSError:
            pass
        raise
    table = SSTable(path)
    db._publish_flush(frozen, table)

# Background compaction: one single pass at a time (background thread plus
# possible compact_now() calls must not overlap).  The victim snapshot is
# immutable; heavy merge I/O happens without the write lock, so reads keep
# flowing and writes keep entering.  Publication is an atomic list swap under
# the write lock followed by a fsynced MANIFEST.

def _compaction_loop(db: "DB") -> None:
    while True:
        with db._compaction_cv:
            while True:
                if db._closing:
                    return
                with db._rw:
                    over = any(len(level) >= db.level0_size for level in db._levels)
                if over:
                    break
                db._compaction_cv.wait(timeout=0.1)
        try:
            merged = _compact_once(db)
            if not merged:
                with db._compaction_cv:
                    db._compaction_cv.wait(timeout=0.05)
        except Exception:  # pragma: no cover - keep the thread alive
            import logging

            logging.getLogger("lsmkv").exception("compaction failed")
            with db._compaction_cv:
                db._compaction_cv.wait(timeout=0.5)


def _compact_once(db: "DB") -> bool:
    # Serialise whole passes so two compactors can never pick overlapping
    # victim sets and then discard each other's outputs.
    if not db._compact_lock.acquire(blocking=False):
        return False
    try:
        with db._rw:
            if db._closed:
                return False
            level_no = -1
            for i, level in enumerate(db._levels):
                if len(level) >= db.level0_size:
                    level_no = i
            if level_no < 0:
                return False
            victims = list(db._levels[level_no])
            target_level_no = level_no + 1
            out_id = db._alloc_table_id_locked()
            # Do NOT append the (possibly empty) target level up front: a flush
            # in the meantime would persist a manifest containing an empty
            # tier.  The level is created atomically at publication below.
            target_exists = target_level_no < len(db._levels)
            target_empty = (not target_exists) or not db._levels[target_level_no]
            deeper_live = any(
                db._levels[i]
                for i in range(target_level_no + 1, len(db._levels))
            )
            drop_tombstones = target_empty and not deeper_live

        # k-way merge, victim segments ordered newest -> oldest; the first
        # occurrence of a key is its newest surviving version.
        heap: list[tuple[bytes, int, Entry, Iterator[Entry]]] = []
        for rank, table in enumerate(victims):
            src = table.entries()
            for first in src:
                heap.append((first.key, rank, first, src))
                break
        heapq.heapify(heap)

        # Resolve the newest version per key into memory first; the merged
        # tier is one segment of comparable size to the inputs, so this is
        # bounded.  It also lets us skip writing an entirely-tombstone merge.
        resolved: list[Entry] = []
        while heap:
            key, rank, entry, src = heapq.heappop(heap)
            while heap and heap[0][0] == key:
                _k, _r, _e, stale = heapq.heappop(heap)
                for nxt in stale:
                    heapq.heappush(heap, (nxt.key, _r, nxt, stale))
                    break
            for nxt in src:
                heapq.heappush(heap, (nxt.key, rank, nxt, src))
                break
            if entry.is_tombstone and drop_tombstones:
                continue
            resolved.append(entry)

        if resolved:
            writer = SSTWriter(target_level_no, out_id, db.directory)
            for entry in resolved:
                writer.add(entry)
            path = writer.finish()

        with db._rw:
            # Membership of a level can still change via flush only for L0;
            # flushes only *insert* newer segments, never delete our victims.
            victim_id_set = {t.table_id for t in victims}
            current_ids = {t.table_id for t in db._levels[level_no]}
            if not victim_id_set.issubset(current_ids):
                if resolved:
                    path.unlink(missing_ok=True)
                return False
            db._levels[level_no] = [
                t for t in db._levels[level_no] if t.table_id not in victim_id_set
            ]
            if resolved:
                merged_table = SSTable(path)
                db._all_tables.add(merged_table)
                while len(db._levels) <= target_level_no:
                    db._levels.append([])
                db._levels[target_level_no].insert(0, merged_table)
            db._levels = [lv for lv in db._levels if lv]
            db._persist_manifest_locked()
        for table in victims:
            table.path.unlink(missing_ok=True)
            db._all_tables.discard(table)
        db._fsync_dir()
        db._stats["compactions"] += 1
        return True
    finally:
        db._compact_lock.release()
