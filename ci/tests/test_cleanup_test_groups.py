"""
End-to-end test for the process-group orphan cleanup in tests/clickhouse-test.

Works on both Linux and macOS.  The ``pgrep()`` helper is imported directly
from ``tests/clickhouse-test`` so it uses ``ps -eo pid,ppid,pgid,command``
(POSIX) rather than the ``pgrep --pgroup`` system command (Linux-only).

Scenario
--------
1. A "parent" process (analogous to clickhouse-test) starts a subprocess
   (the "test process") in its own process group and records the PGID in the
   group pid file — exactly as ``_track_pgid`` does on test launch.
2. The parent is killed with ``SIGKILL``, leaving the test process orphaned
   (and the PGID entry in the file, because ``_untrack_pgid`` never ran).
3. We assert the test process is still alive: it lives in its own process
   group, so the parent's ``SIGKILL`` cannot reach it.
4. We run ``clickhouse-test --cleanup``, which reads the file and kills all
   recorded process groups.
5. We assert the test process is now dead and the pid file is gone.
"""

import fcntl
import os
import runpy
import signal
import subprocess
import sys
import textwrap
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SCRIPT = str(_REPO_ROOT / "tests" / "clickhouse-test")

# Must match the path that clickhouse-test itself computes from its own __file__.
_GROUP_PID_FILE = _REPO_ROOT / "ci" / "tmp" / "clickhouse_test_group_pid"


# pgrep() from clickhouse-test uses ``ps -eo pid,ppid,pgid,command`` and
# works on both Linux and macOS, unlike the ``pgrep --pgroup`` system command.
# runpy.run_path handles the missing .py extension and the hyphen in the name.
pgrep = runpy.run_path(_SCRIPT)["pgrep"]

# ---------------------------------------------------------------------------
# Helper script that plays the role of clickhouse-test's test-runner loop.
# It launches a long-running subprocess in its own process group, writes the
# PGID to the group pid file (mirroring _track_pgid), prints the child PID to
# stdout so the test harness can find the process, then stays alive.  When
# killed with SIGKILL it leaves the subprocess running (and the PGID in the
# file), reproducing the real OOM / external-kill scenario.
# ---------------------------------------------------------------------------

_PARENT_SCRIPT = textwrap.dedent(
    """\
    import fcntl, os, subprocess, sys, time
    from pathlib import Path

    pid_file = Path(sys.argv[1])
    pid_file.parent.mkdir(parents=True, exist_ok=True)

    # Start a test subprocess in its own process group — exactly as clickhouse-test
    # does for every .sh test via start_new_session=True (pgid == pid).
    proc = subprocess.Popen(['sleep', '300'], start_new_session=True)
    pgid = os.getpgid(proc.pid)

    # Track the PGID in the group pid file (mirrors _track_pgid).
    with open(pid_file, 'a') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(f'{pgid}\\n')

    # Signal the test harness that the child is ready.
    print(proc.pid, flush=True)

    # Stay alive — simulates clickhouse-test continuing to run other tests.
    time.sleep(300)
    """
)


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def test_cleanup_kills_orphaned_test_process(tmp_path):
    """
    Verify that ``clickhouse-test --cleanup`` kills a test subprocess that was
    orphaned when its parent (clickhouse-test) was terminated with SIGKILL.
    """
    _GROUP_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    _GROUP_PID_FILE.unlink(missing_ok=True)

    helper_py = tmp_path / "fake_clickhouse_test.py"
    helper_py.write_text(_PARENT_SCRIPT)

    # Start the helper (simulated clickhouse-test) as an ordinary subprocess.
    parent = subprocess.Popen(
        [sys.executable, str(helper_py), str(_GROUP_PID_FILE)],
        stdout=subprocess.PIPE,
        text=True,
    )

    test_pid = None
    try:
        # Wait until the helper has started the test subprocess and written the
        # PGID to the file.
        line = parent.stdout.readline()
        assert line.strip().isdigit(), f"expected a PID on stdout, got: {line!r}"
        test_pid = int(line.strip())

        # Confirm the pid file exists and contains the right PGID.
        assert _GROUP_PID_FILE.exists(), "group pid file should exist after test launch"
        assert str(test_pid) in _GROUP_PID_FILE.read_text(), (
            "test subprocess PGID should be recorded in the group pid file"
        )

        # Verify via pgrep() (imported from clickhouse-test) that the test
        # subprocess belongs to the expected process group.  With
        # start_new_session=True the child's PGID equals its PID.
        assert pgrep(pgid=test_pid), (
            "pgrep() should find at least one process in the test subprocess's group"
        )

        # Kill the parent with SIGKILL — this is the failure mode we are
        # guarding against (OOM killer, external timeout, etc.).
        os.kill(parent.pid, signal.SIGKILL)
        parent.wait(timeout=5)

        # The test subprocess must still be alive.  It lives in its own process
        # group (start_new_session=True), so the parent's SIGKILL cannot reach it.
        assert _is_alive(test_pid), (
            "test subprocess should still be alive after its parent was killed with SIGKILL"
        )

        # Run clickhouse-test --cleanup to kill the orphaned process group.
        result = subprocess.run(
            [sys.executable, _SCRIPT, "--cleanup"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, (
            f"clickhouse-test --cleanup failed (rc={result.returncode}):\n"
            f"{result.stdout}\n{result.stderr}"
        )

        # The test subprocess must now be dead.
        assert not _is_alive(test_pid), (
            "test subprocess should be dead after clickhouse-test --cleanup"
        )

        # The pid file must have been removed by --cleanup.
        assert not _GROUP_PID_FILE.exists(), (
            "group pid file should be deleted by clickhouse-test --cleanup"
        )

    finally:
        # Best-effort cleanup so stray processes are never left behind.
        _GROUP_PID_FILE.unlink(missing_ok=True)
        if test_pid is not None:
            try:
                # pgid == pid because the helper used start_new_session=True.
                os.killpg(test_pid, signal.SIGKILL)
            except OSError:
                pass
        try:
            os.kill(parent.pid, signal.SIGKILL)
            parent.wait(timeout=2)
        except OSError:
            pass
