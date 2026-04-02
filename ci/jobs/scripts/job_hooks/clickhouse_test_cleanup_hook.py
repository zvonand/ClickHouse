"""Post-hook: kill any clickhouse-test processes that outlived fast_test.py.

fast_test.py normally kills all .sh test subprocesses in its finally block via
_kill_session().  But if fast_test.py itself is killed with SIGKILL (e.g. OOM),
that block never runs.  clickhouse-test calls os.setsid() on startup, so all
its descendants share a session whose ID equals clickhouse-test's PID.  The
session file written by run_test() before process.wait() records that ID.  This
hook reads it and kills the session, then removes the file.
"""

import os
import signal
import subprocess
import sys
from pathlib import Path

repo_path = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.append(str(repo_path))

from ci.praktika.utils import Utils

session_file = Path(Utils.cwd()) / "ci/tmp/clickhouse_test_session_id"

if not session_file.exists():
    print("No leftover clickhouse-test session file found, nothing to clean up.")
    sys.exit(0)

try:
    session_id = int(session_file.read_text().strip())
except ValueError as e:
    print(f"Warning: could not parse session ID from {session_file}: {e}")
    session_file.unlink(missing_ok=True)
    sys.exit(0)

print(f"Post-hook: cleaning up session {session_id} left by fast_test.py")

try:
    # ps -o sess is unreliable on macOS (shows tty device number, not session ID),
    # so enumerate all PIDs and use os.getsid() directly — works on both platforms.
    result = subprocess.run(
        ["ps", "-A", "-o", "pid="],
        capture_output=True,
        text=True,
    )
    pids = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line.isdigit():
            continue
        pid = int(line)
        try:
            if os.getsid(pid) == session_id:
                pids.append(pid)
        except (ProcessLookupError, PermissionError):
            pass
    if pids:
        print(f"Killing {len(pids)} leftover process(es): {pids}")
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    else:
        print("No processes found in that session.")
except Exception as e:
    print(f"Warning: session cleanup failed: {e}")

session_file.unlink(missing_ok=True)
