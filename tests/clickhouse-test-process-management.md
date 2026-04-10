# clickhouse-test process management

## Problem: orphan processes when the runner dies unexpectedly

### Original design

`clickhouse-test` moved itself into a new process group on startup:

```python
os.setpgid(0, 0)   # new process group, same session as caller
```

Each `.sh` test was spawned in a completely new **session**:

```python
Popen(command, shell=True, start_new_session=True, ...)
```

`start_new_session=True` is equivalent to calling `setsid()` in the child before `exec`.
The test process becomes a session leader in a brand-new session with no relationship
to any other process group or session.

### Why `start_new_session=True` is used

When bash runs a pipeline it does not forward signals to its children by default.
If you send SIGTERM to a bash process, the children (ClickHouse client, grep, awk, …)
survive.  `os.killpg(pgid, SIGKILL)` solves this: it kills every process in the group
simultaneously, regardless of signal forwarding.  For that to work each test's subprocess
tree must be in a known, dedicated process group — hence `start_new_session=True` (which
makes `PGID = PID` for the spawned shell).

### The orphan gap

`cleanup_child_processes` in `clickhouse-test` uses `pgrep --parent {pid}` to find
direct children, then calls `os.killpg` on each child's process group.  This works
when `clickhouse-test` is alive and its signal handlers run (SIGTERM, SIGINT, SIGHUP).

If `clickhouse-test` is killed with **SIGKILL** or dies due to an OOM event, its signal
handlers never run.  The `.sh` test subprocesses are re-parented to `init`/`launchd`,
but they keep running.  Because they are in separate sessions, they are unreachable via
the parent's process group.  No kernel mechanism automatically reaps them.

This was not a problem when running inside Docker: the container's PID namespace acts as
a hard boundary — when the container exits, every process inside dies regardless of
sessions or process groups.

On macOS (no Docker, no cgroups, no `prctl(PR_SET_PDEATHSIG)`), orphans accumulate
across test runs.

### Secondary gap: nested sessions via `timeout`

GNU `timeout` calls `setsid()` on the child it wraps, creating a grandchild session.
`cleanup_child_processes` only walks one level deep (`pgrep --parent`), so grandchildren
in nested sessions can survive a graceful cleanup too.  In practice this is bounded:
`timeout` will eventually kill its child when the timer fires, even if the parent is gone.

---

## Solution: PGID-based tracking

The fix keeps `start_new_session=True` for `.sh` test subprocesses (their PGID = their PID)
and adds explicit PGID bookkeeping so the caller can kill orphans by process group ID.

### How the kernel stores the PGID

The kernel records the PGID directly in the process descriptor (`task_struct` on Linux,
`proc` entry on macOS/BSD).  It is **never reset** when a process is re-parented to
`init`/`launchd`.  Therefore `os.killpg(pgid, SIGKILL)` reaches a re-parented orphan
as long as you know its PGID — no session traversal needed.

### `clickhouse-test` startup

```python
# before (our change)
os.setsid()        # was changed to setsid to enable session-based tracking — no longer needed

# after (current)
if os.getpid() != os.getpgid(0):
    os.setpgid(0, 0)   # new process group, same session; isolates from terminal signals
```

`setsid` was only needed for session-based orphan tracking.  With PGID tracking we no
longer need to move to a new session — `setpgid(0, 0)` is enough to prevent terminal
signals from reaching the caller.  If the caller already used `start_new_session=True`
(as `run_test()` does), `clickhouse-test` is already a process group leader and the
`setpgid` call raises `PermissionError`, which we silently ignore.

### Per-test PGID bookkeeping

```python
proc = Popen(command, shell=True, start_new_session=True, preexec_fn=cgroup_fn)
_track_pgid(proc.pid)   # proc.pid == PGID after start_new_session=True
```

```python
# in process_result_impl, after the test exits or is killed:
_untrack_pgid(proc.pid)
```

`_track_pgid` appends the PGID to `_GROUP_PID_FILE` under an `fcntl.LOCK_EX` lock so
parallel test workers do not corrupt each other's writes.  `_untrack_pgid` removes the
entry under the same lock.

### Group pid file

`_GROUP_PID_FILE` = `{repo}/ci/tmp/clickhouse_test_group_pid`

The file contains one PGID per line.  On a clean run it is empty (or absent) when
`clickhouse-test` exits because every test that started has called `_untrack_pgid`.
If `clickhouse-test` is killed with SIGKILL, the file retains the PGIDs of still-running
test processes.

### Caller cleanup (`run_test` in `clickhouse_proc.py`)

After `process.wait()` returns (however it exits), the caller invokes:

```python
subprocess.run(["clickhouse-test", "--cleanup"], check=False)
```

`clickhouse-test --cleanup` calls `cleanup_test_groups()`:

```python
for pgid in pgids_from_file:
    os.killpg(pgid, signal.SIGKILL)
_GROUP_PID_FILE.unlink(missing_ok=True)
```

### Post-hook guard: `fast_test.py` killed with SIGKILL

The `finally` block in `run_test` is Python-level and does not run if `fast_test.py`
itself is killed with SIGKILL or OOM-killed.  A post-hook registered on the
`darwin_fast_test_jobs` job definition runs after the job script exits regardless of
how it died.  It calls `clickhouse-test --cleanup` directly:

```
ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py
```

The hook is registered in `ci/defs/job_configs.py`:

```python
darwin_fast_test_jobs = Job.Config(
    ...
    post_hooks=["python3 ./ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py"],
)
```

The hook has no cleanup logic of its own — it just calls `clickhouse-test --cleanup`,
which is the single source of truth for orphan cleanup.

### Cleanup layers, innermost to outermost

| Layer | Trigger | Mechanism |
|---|---|---|
| `clickhouse-test` `cleanup_child_processes` | SIGTERM/SIGINT/SIGHUP to the runner | `killpg` on each child's PGID, one level deep |
| `run_test()` `finally` block | Any exit of `clickhouse-test` (incl. SIGKILL) | `clickhouse-test --cleanup` → `killpg` per PGID in group pid file |
| Post-hook `clickhouse_test_cleanup_hook.py` | Any exit of `fast_test.py` (incl. SIGKILL) | same — `clickhouse-test --cleanup` |

### What each failure mode looks like

| Scenario | Before | After |
|---|---|---|
| Normal exit | Cleaned up by `clickhouse-test` signal handlers | Same; `_untrack_pgid` keeps the file current so `--cleanup` has nothing to do |
| `clickhouse-test` SIGKILL / OOM | Orphans survive indefinitely | `run_test()` `finally` calls `clickhouse-test --cleanup`; kills by PGID from file |
| `fast_test.py` SIGKILL / OOM | Orphans survive indefinitely | Post-hook calls `clickhouse-test --cleanup`; same mechanism |
| Runner process (`runner.py`) killed | Orphans survive indefinitely | Not covered — would need an external watchdog or cgroup |
| GNU `timeout` grandchildren | Killed by `timeout` on expiry | Same (unchanged, bounded by timeout) |

### Remaining limitation

If the runner process (`runner.py`) itself is killed before the post-hook executes,
nothing cleans up.  On a dedicated macOS CI runner this is an extreme failure mode
(full machine OOM or kernel panic); a reboot clears all processes anyway.  For Linux
production CI the existing Docker/cgroup boundary already covers this.
