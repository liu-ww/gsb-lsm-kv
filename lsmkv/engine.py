"""The LSM-tree storage engine.

Write path:  put/delete -> append WAL record (crc32 + fsync) -> insert into
the skip-list memtable.  When the memtable grows past ``memtable_threshold``
bytes it is frozen, handed to a single background thread that writes it out
as a tier-0 SSTable segment, and only then is its WAL file deleted.

Read path:  active memtable -> immutable memtables (newest first) ->
tier 0 segments (newest first) -> tier 1 -> ...  The first hit wins, which
is the newest version because data only ever moves from newer to older
tiers and compaction always merges *whole* tiers.

Compaction: size-tiered.  When a tier accumulates ``TIER_FANOUT`` segments
the whole tier is merged into one segment in the next tier.  During the
merge the highest-seqno version of each key wins; tombstones are dropped
only when no deeper tier exists that could still hold older versions.
Readers and writers are never blocked by compaction: they snapshot the
segment list under a short lock and compaction atomically swaps the list.
"""

from __future__ import annotations

import heapq
import os
import re
import threading
from typing import Dict, Iterator, List, NamedTuple, Optional, Tuple, Union

from .skiplist import Record, SkipList
from .sstable import SstableReader, SstableWriter
from .wal import WalWriter, replay

BytesLike = Union[bytes, bytearray, memoryview, str]

MEMTABLE_THRESHOLD = 4 * 1024 * 1024  # 4 MiB
TIER_FANOUT = 4

_WAL_RE = re.compile(r"wal-(\d+)\.log$")
_SEG_RE = re.compile(r"tier(\d+)-seg(\d+)\.sst$")


def _as_bytes(value: BytesLike) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"expected bytes-like or str, got {type(value).__name__}")


class _SourceItem(NamedTuple):
    key: bytes
    seqno: int
    value: bytes
    tombstone: bool


class DB:
    """An embedded LSM-tree key-value store.  Thread-safe."""

    def __init__(
        self,
        path: str,
        *,
        memtable_threshold: int = MEMTABLE_THRESHOLD,
        tier_fanout: int = TIER_FANOUT,
    ) -> None:
        self.path = path
        self.memtable_threshold = memtable_threshold
        self.tier_fanout = tier_fanout
        os.makedirs(path, exist_ok=True)

        self._wal_lock = threading.Lock()
        self._mem_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._cond = threading.Condition(self._state_lock)

        self._memtable = SkipList()
        self._immutables: List[Tuple[List[int], SkipList]] = []  # (wal ids, table), oldest first
        self._tiers: List[List[SstableReader]] = []
        self._seqno = 0
        self._next_id = 1
        self._stop = False
        self._closed = False

        self._recover()

        with self._state_lock:
            self._wal_ids_covered_by_memtable = self._recovered_wal_ids
            wal_id = self._alloc_file_id()
        self._wal_id = wal_id
        self._wal = WalWriter(self._wal_path(wal_id))
        self._wal_ids_covered_by_memtable.append(wal_id)

        # If recovery filled the memtable past the threshold, rotate now.
        if self._memtable.approx_size >= self.memtable_threshold:
            self._rotate_memtable(force=True)

        self._bg = threading.Thread(target=self._bg_loop, name="lsmkv-bg", daemon=True)
        self._bg.start()

    # ------------------------------------------------------------------ #
    # file naming / recovery
    # ------------------------------------------------------------------ #
    def _wal_path(self, wal_id: int) -> str:
        return os.path.join(self.path, f"wal-{wal_id:06d}.log")

    def _seg_path(self, tier: int, seg_id: int) -> str:
        return os.path.join(self.path, f"tier{tier}-seg{seg_id:06d}.sst")

    def _alloc_file_id(self) -> int:
        """Must be called with ``_state_lock`` held."""
        fid = self._next_id
        self._next_id += 1
        return fid

    def _recover(self) -> None:
        max_seqno = 0
        wal_ids: List[int] = []
        segs: List[Tuple[int, int, str]] = []  # (tier, id, path)
        for name in os.listdir(self.path):
            m = _WAL_RE.match(name)
            if m:
                wal_ids.append(int(m.group(1)))
                continue
            m = _SEG_RE.match(name)
            if m:
                segs.append((int(m.group(1)), int(m.group(2)), os.path.join(self.path, name)))

        for tier, seg_id, seg_path in segs:
            reader = SstableReader(seg_path, seg_id, tier)
            while len(self._tiers) <= tier:
                self._tiers.append([])
            self._tiers[tier].append(reader)
            max_seqno = max(max_seqno, reader.max_seqno)
            self._next_id = max(self._next_id, seg_id + 1)
        for tier_list in self._tiers:
            tier_list.sort(key=lambda r: r.segment_id)

        for wal_id in sorted(wal_ids):
            self._next_id = max(self._next_id, wal_id + 1)
            for entry in replay(self._wal_path(wal_id)):
                self._memtable.put(
                    entry.key, Record(entry.value, entry.seqno, entry.tombstone)
                )
                max_seqno = max(max_seqno, entry.seqno)

        self._seqno = max_seqno
        self._recovered_wal_ids = sorted(wal_ids)

    # ------------------------------------------------------------------ #
    # write path
    # ------------------------------------------------------------------ #
    def put(self, key: BytesLike, value: BytesLike) -> None:
        k = _as_bytes(key)
        v = _as_bytes(value)
        with self._wal_lock:
            self._check_open()
            self._seqno += 1
            seqno = self._seqno
            self._wal.append(k, v, seqno, tombstone=False)
            with self._mem_lock:
                self._memtable.put(k, Record(v, seqno, False))
                over = self._memtable.approx_size >= self.memtable_threshold
            if over:
                self._rotate_memtable()

    def delete(self, key: BytesLike) -> None:
        k = _as_bytes(key)
        with self._wal_lock:
            self._check_open()
            self._seqno += 1
            seqno = self._seqno
            self._wal.append(k, b"", seqno, tombstone=True)
            with self._mem_lock:
                self._memtable.put(k, Record(b"", seqno, True))
                over = self._memtable.approx_size >= self.memtable_threshold
            if over:
                self._rotate_memtable()

    def _rotate_memtable(self, force: bool = False) -> None:
        """Freeze the active memtable and start a fresh WAL.

        Called with ``_wal_lock`` held (or during recovery/close).
        """
        with self._mem_lock:
            if not force and self._memtable.approx_size < self.memtable_threshold:
                return
            if len(self._memtable) == 0:
                return
            old_wal = self._wal
            old_wal_ids = self._wal_ids_covered_by_memtable
            old_mem = self._memtable
            with self._state_lock:
                new_wal_id = self._alloc_file_id()
            self._memtable = SkipList()
            self._wal_ids_covered_by_memtable = [new_wal_id]
            self._wal_id = new_wal_id
            self._wal = WalWriter(self._wal_path(new_wal_id))
            old_wal.close()
            with self._cond:
                self._immutables.append((old_wal_ids, old_mem))
                self._cond.notify_all()

    # ------------------------------------------------------------------ #
    # read path
    # ------------------------------------------------------------------ #
    def get(self, key: BytesLike) -> Optional[bytes]:
        """Return the value for ``key``, or ``None`` if the key does not
        exist or was deleted.  A stored empty value returns ``b""``, which
        is distinct from ``None``."""
        k = _as_bytes(key)
        with self._mem_lock:
            rec = self._memtable.get(k)
        if rec is not None:
            return None if rec.tombstone else rec.value
        with self._state_lock:
            immutables = list(reversed(self._immutables))  # newest first
            tiers = [list(t) for t in self._tiers]
        for _, mem in immutables:
            rec = mem.get(k)
            if rec is not None:
                return None if rec.tombstone else rec.value
        for tier in tiers:
            for seg in sorted(tier, key=lambda r: -r.segment_id):
                entry = seg.get(k)
                if entry is not None:
                    return None if entry.tombstone else entry.value
        return None

    def scan(self, prefix: BytesLike = b"") -> Iterator[Tuple[bytes, bytes]]:
        """Lazily yield ``(key, value)`` pairs with the given prefix, in key
        order.  Deleted keys are filtered out."""
        p = _as_bytes(prefix)
        with self._mem_lock:
            mem = self._memtable
            with self._state_lock:
                immutables = list(self._immutables)
                tiers = [list(t) for t in self._tiers]

        def mem_items(table: SkipList) -> Iterator[_SourceItem]:
            for key, rec in table.items():
                yield _SourceItem(key, rec.seqno, rec.value, rec.tombstone)

        sources: List[Iterator[_SourceItem]] = [mem_items(mem)]
        for _, table in immutables:
            sources.append(mem_items(table))
        for tier in tiers:
            for seg in tier:
                sources.append(
                    _SourceItem(e.key, e.seqno, e.value, e.tombstone)
                    for e in seg.iter_entries()
                )
        yield from _merge_sources(sources, p)

    # ------------------------------------------------------------------ #
    # background flush + compaction
    # ------------------------------------------------------------------ #
    def _tier_needs_compaction(self) -> bool:
        return any(len(t) >= self.tier_fanout for t in self._tiers)

    def _bg_loop(self) -> None:
        while True:
            with self._cond:
                while (
                    not self._stop
                    and not self._immutables
                    and not self._tier_needs_compaction()
                ):
                    self._cond.wait()
                if self._stop and not self._immutables:
                    return
                work = list(self._immutables)
            for wal_ids, mem in work:
                self._flush_memtable(wal_ids, mem)
            while True:
                with self._cond:
                    tier_idx = next(
                        (i for i, t in enumerate(self._tiers) if len(t) >= self.tier_fanout),
                        None,
                    )
                if tier_idx is None:
                    break
                self._compact_tier(tier_idx)

    def _flush_memtable(self, wal_ids: List[int], mem: SkipList) -> None:
        with self._state_lock:
            seg_id = self._alloc_file_id()
        seg_path = self._seg_path(0, seg_id)
        writer = SstableWriter(seg_path)
        for key, rec in mem.items():
            writer.add(key, rec.value, rec.seqno, rec.tombstone)
        writer.finish()
        reader = SstableReader(seg_path, seg_id, 0)
        with self._cond:
            self._immutables = [item for item in self._immutables if item[1] is not mem]
            while len(self._tiers) <= 0:
                self._tiers.append([])
            self._tiers[0].append(reader)
            self._cond.notify_all()
        for wal_id in wal_ids:
            try:
                os.unlink(self._wal_path(wal_id))
            except FileNotFoundError:
                pass

    def _compact_tier(self, tier_idx: int) -> None:
        with self._state_lock:
            segments = list(self._tiers[tier_idx])
            deeper_empty = all(
                not self._tiers[i] for i in range(tier_idx + 1, len(self._tiers))
            )
        sources = [
            (_SourceItem(e.key, e.seqno, e.value, e.tombstone) for e in seg.iter_entries())
            for seg in segments
        ]
        with self._state_lock:
            out_id = self._alloc_file_id()
        out_path = self._seg_path(tier_idx + 1, out_id)
        writer = SstableWriter(out_path)
        for item in _merge_items(sources, drop_tombstones=deeper_empty):
            writer.add(item.key, item.value, item.seqno, item.tombstone)
        writer.finish()
        reader: Optional[SstableReader] = None
        if writer.entry_count > 0:
            reader = SstableReader(out_path, out_id, tier_idx + 1)
        else:
            os.unlink(out_path)
        merged_ids = {id(s) for s in segments}
        with self._cond:
            self._tiers[tier_idx] = [
                s for s in self._tiers[tier_idx] if id(s) not in merged_ids
            ]
            if reader is not None:
                while len(self._tiers) <= tier_idx + 1:
                    self._tiers.append([])
                self._tiers[tier_idx + 1].append(reader)
            self._cond.notify_all()
        # Do not close the old segment fds here: in-flight readers may still
        # hold the pre-compaction snapshot of the tier list.  Unlinking is
        # safe on POSIX; the fd is released once the last reference drops.
        for seg in segments:
            try:
                os.unlink(seg.path)
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def flush(self) -> None:
        """Force the active memtable to be flushed to an SSTable and wait
        for all pending flushes/compactions to finish."""
        with self._wal_lock:
            self._check_open()
            self._rotate_memtable(force=True)
        self.wait_for_background()

    def wait_for_background(self) -> None:
        with self._cond:
            while self._immutables or self._tier_needs_compaction():
                self._cond.wait()

    @property
    def segment_count(self) -> int:
        with self._state_lock:
            return sum(len(t) for t in self._tiers)

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("DB is closed")

    def close(self) -> None:
        if self._closed:
            return
        with self._wal_lock:
            self._rotate_memtable(force=True)
            self._closed = True
            self._wal.close()
            wal_path = self._wal_path(self._wal_id)
        with self._cond:
            self._stop = True
            self._cond.notify_all()
        self._bg.join()
        # The last WAL belonged to an empty memtable; nothing references it.
        try:
            if os.path.getsize(wal_path) == 0:
                os.unlink(wal_path)
        except FileNotFoundError:
            pass
        with self._state_lock:
            tiers = [s for t in self._tiers for s in t]
        for seg in tiers:
            seg.close()

    def __enter__(self) -> "DB":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _merge_sources(
    sources: List[Iterator[_SourceItem]], prefix: bytes
) -> Iterator[Tuple[bytes, bytes]]:
    for item in _merge_items(sources, drop_tombstones=True):
        if item.key.startswith(prefix):
            yield item.key, item.value


def _merge_items(
    sources: List[Iterator[_SourceItem]], drop_tombstones: bool
) -> Iterator[_SourceItem]:
    """K-way merge of sorted sources.  For each key the highest-seqno
    version wins; tombstones are dropped only if ``drop_tombstones``."""
    heap: List[Tuple[bytes, int, int, _SourceItem, Iterator[_SourceItem]]] = []
    counter = 0

    def advance(it: Iterator[_SourceItem]) -> None:
        nonlocal counter
        try:
            item = next(it)
        except StopIteration:
            return
        heapq.heappush(heap, (item.key, -item.seqno, counter, item, it))
        counter += 1

    for it in sources:
        advance(it)
    while heap:
        key, _, _, best, best_it = heapq.heappop(heap)
        advance(best_it)
        # Discard every older version of this same key.
        while heap and heap[0][0] == key:
            _, _, _, _, other_it = heapq.heappop(heap)
            advance(other_it)
        if best.tombstone and drop_tombstones:
            continue
        yield best
