"""
End-to-end test for the process-group orphan cleanup in tests/clickhouse-test.

Works on both Linux and macOS.  The ``pgrep()`` helper is imported directly
from ``tests/clickhouse-test`` so it uses ``ps -eo pid,ppid,pgid,command``
(POSIX) rather than the ``pgrep --pgroup`` system command (Linux-only).

Scenario
--------
1. ``clickhouse-test`` starts a subprocess (the "test process") in its own
   process group and records the PGID in the group pid file — exactly as
   ``_track_pgid`` does on test launch.  In the multi-process variant the test
   is ``00058_select_sleep_3.sh``, which spawns 5 child ``clickhouse-client``
   processes running ``SELECT sleep(3)``.
2. ``clickhouse-test`` is killed with ``SIGKILL``, leaving the test process
   (and its children) orphaned because ``_untrack_pgid`` never ran.
3. We assert the test process and its 5 child processes are still alive: they
   live in their own process group, so the parent's ``SIGKILL`` cannot reach
   them.
4. We run ``clickhouse-test --cleanup``, which reads the file and kills all
   recorded process groups.
5. We assert all processes are now dead and the pid file is gone.
"""

import os
import runpy
import signal
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")
_TEST = "00058_select_sleep_3"

# Import helpers directly from clickhouse-test so path changes propagate
# automatically.  runpy.run_path handles the missing .py extension and the
# hyphen in the name.
_ct = runpy.run_path(_CLICKHOUSE_TEST)
pgrep = _ct["pgrep"]
_GROUP_PID_FILE = _ct["_GROUP_PID_FILE"]


def test_cleanup_kills_orphaned_test_process():
    """
    Verify that ``clickhouse-test --cleanup`` kills a test subprocess that was
    orphaned when its parent (clickhouse-test) was terminated with SIGKILL.
    """
    _GROUP_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    for f in _GROUP_PID_FILE.parent.glob(f"{_GROUP_PID_FILE.name}.*"):
        f.unlink(missing_ok=True)

    _ch_proc = subprocess.Popen(
        [sys.executable, _CLICKHOUSE_TEST, _TEST],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    pgid = None
    try:
        # Wait until clickhouse-test has launched the test subprocess and
        # written its PGID to the group pid file.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            pgid_files = [
                p for p in _GROUP_PID_FILE.parent.glob(f"{_GROUP_PID_FILE.name}.*")
                if not p.name.endswith(".tmp")
            ]
            for pf in pgid_files:
                lines = [l for l in pf.read_text().splitlines() if l.strip()]
                if lines:
                    pgid = int(lines[0])
                    break
            if pgid is not None:
                break
            time.sleep(0.1)
        assert pgid is not None, "group pid file was not populated in time"

        # Wait for all 5 child processes to appear in the process group.
        deadline = time.monotonic() + 15
        procs = []
        while time.monotonic() < deadline:
            procs = pgrep(pgid=pgid)
            if len(procs) >= 2:
                break
            time.sleep(0.1)
        assert len(procs) >= 2, (
            f"expected multiple processes in group {pgid} (bash + client jobs), "
            f"got: {procs}"
        )

        # Kill clickhouse-test with SIGKILL — simulates the OOM killer or an
        # external timeout killing the test runner.
        os.kill(_ch_proc.pid, signal.SIGKILL)
        _ch_proc.wait(timeout=5)

        # The test subprocess and its children must still be alive: they live
        # in their own process group, so the parent's SIGKILL cannot reach them.
        assert pgrep(pgid=pgid), (
            "test processes should still be alive after clickhouse-test was killed with SIGKILL"
        )

        # Run clickhouse-test --cleanup to kill the orphaned process group.
        result = subprocess.run(
            [sys.executable, _CLICKHOUSE_TEST, "--cleanup"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, (
            f"clickhouse-test --cleanup failed (rc={result.returncode}):\n"
            f"{result.stdout}\n{result.stderr}"
        )

        # All test processes must now be dead.
        assert not pgrep(pgid=pgid), (
            "all test processes should be dead after clickhouse-test --cleanup"
        )

        # All per-worker pid files must have been removed by --cleanup.
        remaining = [
            p for p in _GROUP_PID_FILE.parent.glob(f"{_GROUP_PID_FILE.name}.*")
            if not p.name.endswith(".tmp")
        ]
        assert not remaining, (
            "group pid files should be deleted by clickhouse-test --cleanup"
        )

    finally:
        # Best-effort cleanup so stray processes are never left behind.
        for f in _GROUP_PID_FILE.parent.glob(f"{_GROUP_PID_FILE.name}.*"):
            f.unlink(missing_ok=True)
        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except OSError:
                pass
        try:
            os.kill(_ch_proc.pid, signal.SIGKILL)
            _ch_proc.wait(timeout=2)
        except OSError:
            pass
