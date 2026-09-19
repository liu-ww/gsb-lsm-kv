"""Child process for the crash-recovery test.

Writes a deterministic dataset (puts, overwrites, deletes) and then dies
with os._exit(1) to simulate a hard crash (kill -9).  Every put/delete is
WAL-fsynced before returning, so all acknowledged writes must survive.
"""

import os
import sys

if __name__ == "__main__":
    sys.path.insert(0, os.environ["LSMKV_REPO"])
    from lsmkv import DB
else:
    from lsmkv import DB  # noqa: E402


def expected_state(n: int) -> dict:
    """The deterministic final state both sides compute independently."""
    state = {}
    for i in range(n):
        state[b"key%06d" % i] = b"v1-%06d" % i
    for i in range(0, n, 2):  # overwrite even keys
        state[b"key%06d" % i] = b"v2-%06d" % i
    for i in range(0, n, 3):  # delete every third key
        del state[b"key%06d" % i]
    return state


def main() -> None:
    db_path = sys.argv[1]
    n = int(sys.argv[2])
    db = DB(db_path)
    for i in range(n):
        db.put(b"key%06d" % i, b"v1-%06d" % i)
    for i in range(0, n, 2):
        db.put(b"key%06d" % i, b"v2-%06d" % i)
    for i in range(0, n, 3):
        db.delete(b"key%06d" % i)
    # Hard crash: no close(), no cleanup, no atexit handlers.
    os._exit(1)


if __name__ == "__main__":
    main()
