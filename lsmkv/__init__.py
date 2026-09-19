"""lsmkv: an embedded LSM-tree key-value store in pure Python stdlib."""

from .engine import DB, MEMTABLE_THRESHOLD, TIER_FANOUT
from .sstable import CorruptionError

__all__ = ["DB", "CorruptionError", "MEMTABLE_THRESHOLD", "TIER_FANOUT"]
__version__ = "0.1.0"
