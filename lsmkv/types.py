"""Shared value types for lsmkv."""

from __future__ import annotations

from dataclasses import dataclass

# Tombstone marker inside memtables and SSTables.
TOMBSTONE: bytes = b""


@dataclass(slots=True, frozen=True)
class Entry:
    """One version of a key.

    ``value == TOMBSTONE`` (``b""`` with ``is_tombstone=True``) represents a
    deletion.  An empty string as a live value is distinguishable because
    ``is_tombstone`` is False.
    """

    key: bytes
    value: bytes
    sequence: int
    is_tombstone: bool
