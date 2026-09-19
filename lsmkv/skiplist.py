"""Hand-written skip list used as the in-memory memtable.

Keys are ``bytes`` and are kept in ascending order.  Each key maps to a
``Record`` named tuple ``(value, seqno, tombstone)``.  The skip list itself
is not internally synchronized; the engine serializes writers and uses a
lock around reads of the *active* memtable.  Iteration is safe against
concurrent inserts because a node's ``forward`` array is fully populated
before the node is linked into the list, and nodes are never removed.
"""

from __future__ import annotations

import random
from typing import Iterator, List, NamedTuple, Optional, Tuple

MAX_LEVEL = 20
P = 0.25


class Record(NamedTuple):
    value: bytes
    seqno: int
    tombstone: bool


class _Node:
    __slots__ = ("key", "record", "forward")

    def __init__(self, key: bytes, record: Optional[Record], level: int) -> None:
        self.key = key
        self.record = record
        self.forward: List[Optional[_Node]] = [None] * level


class SkipList:
    """Ordered map from bytes keys to ``Record`` values."""

    __slots__ = ("_head", "_level", "_size", "_count", "_rng")

    def __init__(self) -> None:
        self._head = _Node(b"", None, MAX_LEVEL)
        self._level = 1
        self._size = 0  # approximate bytes held (keys + values + overhead)
        self._count = 0
        self._rng = random.Random()

    def _random_level(self) -> int:
        level = 1
        while level < MAX_LEVEL and self._rng.random() < P:
            level += 1
        return level

    def get(self, key: bytes) -> Optional[Record]:
        node = self._head
        for i in range(self._level - 1, -1, -1):
            while node.forward[i] is not None and node.forward[i].key < key:  # type: ignore[union-attr]
                node = node.forward[i]  # type: ignore[assignment]
        node = node.forward[0]
        if node is not None and node.key == key:
            return node.record
        return None

    def put(self, key: bytes, record: Record) -> None:
        update: List[Optional[_Node]] = [None] * MAX_LEVEL
        node = self._head
        for i in range(self._level - 1, -1, -1):
            while node.forward[i] is not None and node.forward[i].key < key:  # type: ignore[union-attr]
                node = node.forward[i]  # type: ignore[assignment]
            update[i] = node
        nxt = node.forward[0]
        if nxt is not None and nxt.key == key:
            old = nxt.record
            nxt.record = record
            self._size += len(record.value) - (len(old.value) if old else 0)
            return
        level = self._random_level()
        if level > self._level:
            for i in range(self._level, level):
                update[i] = self._head
            self._level = level
        new_node = _Node(key, record, level)
        # Populate the node's own forward pointers *before* linking it in so
        # concurrent readers never observe a half-linked node.
        for i in range(level):
            new_node.forward[i] = update[i].forward[i]  # type: ignore[union-attr]
        for i in range(level):
            update[i].forward[i] = new_node  # type: ignore[union-attr]
        self._count += 1
        # ~50 bytes accounts for node object + forward pointer overhead.
        self._size += len(key) + len(record.value) + 50

    def __len__(self) -> int:
        return self._count

    @property
    def approx_size(self) -> int:
        """Approximate number of bytes held by this memtable."""
        return self._size

    def items(self) -> Iterator[Tuple[bytes, Record]]:
        node = self._head.forward[0]
        while node is not None:
            yield node.key, node.record  # type: ignore[misc]
            node = node.forward[0]
