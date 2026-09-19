"""The MANIFEST file: durable, atomically replaced list of live segments.

Plain-text, one record per line, little-endian integers in decimal::

    lsmkv-manifest-v1
    seq <next sequence number to allocate>
    next-id <next segment id to allocate>
    level <n> <seg-id> <seg-id> ...     # ids listed newest-first
    ...
    end
"""

from __future__ import annotations

import os
from pathlib import Path

NAME = "MANIFEST"
_MAGIC = "lsmkv-manifest-v1"


def manifest_path(directory: Path) -> Path:
    return directory / NAME


def write(
    directory: Path,
    sequence: int,
    next_id: int,
    levels: list[list[int]],
    sync: bool = True,
) -> None:
    lines = [_MAGIC, f"seq {sequence}", f"next-id {next_id}"]
    for level_no, seg_ids in enumerate(levels):
        lines.append(f"level {level_no} " + " ".join(str(i) for i in seg_ids))
    lines.append("end")
    payload = ("\n".join(lines) + "\n").encode()
    target = manifest_path(directory)
    tmp = directory / "MANIFEST.tmp"
    with open(tmp, "wb", buffering=0) as f:
        f.write(payload)
        f.flush()
        if sync:
            os.fsync(f.fileno())
    os.replace(tmp, target)
    if sync:
        _fsync_dir(directory)


def read(directory: Path) -> tuple[int, int, list[list[int]]]:
    path = manifest_path(directory)
    if not path.exists():
        return 0, 1, []
    lines = path.read_text().splitlines()
    if not lines or lines[0] != _MAGIC:
        raise ValueError(f"bad manifest magic in {path}")
    sequence = 0
    next_id = 1
    levels: list[list[int]] = []
    for line in lines[1:]:
        if line == "end" or not line:
            continue
        parts = line.split()
        if parts[0] == "seq":
            sequence = int(parts[1])
        elif parts[0] == "next-id":
            next_id = int(parts[1])
        elif parts[0] == "level":
            level_no = int(parts[1])
            while len(levels) <= level_no:
                levels.append([])
            levels[level_no] = [int(x) for x in parts[2:]]
        else:
            raise ValueError(f"unknown manifest record: {line!r}")
    return sequence, next_id, levels


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
