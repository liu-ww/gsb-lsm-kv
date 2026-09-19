"""A hand-written skip list used as the in-memory memtable.

No third-party ordered containers are used: this is a classic probabilistic
skip list with forward pointers, a per-node tower and value (key, value,
sequence, tombstone) tuples.
"""

from __future__ import annotations

import random
from typing import Iterator

from .types import Entry

_MAX_LEVEL = 16
_BRANCHING = 0.25
# Bookkeeping overhead charged per inserted entry when estimating memtable size.
_ENTRY_OVERHEAD = 48


class _Node:
    __slots__ = ("key", "value", "sequence", "is_tombstone", "forward")

    def __init__(
        self,
        key: bytes,
        value: bytes,
        sequence: int,
        is_tombstone: bool,
        level: int,
    ) -> None:
        self.key = key
        self.value = value
        self.sequence = sequence
        self.is_tombstone = is_tombstone
        # forward[0] is the bottom level (the ordered linked list).
        self.forward: list[_Node | None] = [None] * level

    def as_entry(self) -> Entry:
        return Entry(self.key, self.value, self.sequence, self.is_tombstone)


class SkipList:
    """Ordered map of bytes keys to :class:`Entry` (latest key wins on put).

    The structure itself is single-writer safe under the database write lock;
    concurrent readers may traverse a frozen (no-longer-mutated) instance with
    no lock at all.  The bottom level is an immutable-ish linked list for
    traversal: updates only change forward pointers of existing nodes, never
    the key/value payload, so a reader already inside a node keeps valid data.
    """

    def __init__(self) -> None:
        self._head = _Node(b"", b"", 0, False, _MAX_LEVEL)
        self._level = 1
        self._len = 0
        self._approx_bytes = 0
        self._rng = random.Random(0x57A7)

    def _random_level(self) -> int:
        level = 1
        while level < _MAX_LEVEL and self._rng.random() < _BRANCHING:
            level += 1
        return level

    def get(self, key: bytes) -> Entry | None:
        node = self._head
        for lev in range(self._level - 1, -1, -1):
            nxt = node.forward[lev]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.forward[lev]
        cur = node.forward[0]
        if cur is not None and cur.key == key:
            return cur.as_entry()
        return None

    def put(self, entry: Entry) -> None:
        update: list[_Node] = [self._head] * _MAX_LEVEL
        node = self._head
        for lev in range(self._level - 1, -1, -1):
            nxt = node.forward[lev]
            while nxt is not None and nxt.key < entry.key:
                node = nxt
                nxt = node.forward[lev]
            update[lev] = node

        cur = node.forward[0]
        if cur is not None and cur.key == entry.key:
            # Overwrite payload in place: newest version of this key.
            self._approx_bytes += len(entry.value) - len(cur.value)
            cur.value = entry.value
            cur.sequence = entry.sequence
            cur.is_tombstone = entry.is_tombstone
            return

        level = self._random_level()
        if level > self._level:
            for lev in range(self._level, level):
                update[lev] = self._head
            self._level = level

        new_node = _Node(entry.key, entry.value, entry.sequence, entry.is_tombstone, level)
        for lev in range(level):
            new_node.forward[lev] = update[lev].forward[lev]
            update[lev].forward[lev] = new_node

        self._len += 1
        self._approx_bytes += len(entry.key) + len(entry.value) + _ENTRY_OVERHEAD

    def __len__(self) -> int:
        return self._len

    def estimate_bytes(self) -> int:
        """Rough in-memory size used to decide when to flush."""
        return self._approx_bytes

    def entries(self) -> Iterator[Entry]:
        """Yield every entry in ascending key order (bottom-level chain)."""
        node = self._head.forward[0]
        while node is not None:
            yield node.as_entry()
            node = node.forward[0]
