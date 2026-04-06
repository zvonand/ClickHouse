# clickhouse-test process management

## Problem: orphan processes when the runner dies unexpectedly

`clickhouse-test` launches each `.sh` test in its own process group via
`start_new_session=True` (which sets `PGID = PID` for the spawned shell).
This is needed so that `os.killpg` can kill an entire test's subprocess tree
at once — bash does not forward signals to its children by default.

`cleanup_child_processes` handles graceful shutdown (SIGTERM/SIGINT/SIGHUP):
it walks direct children with `pgrep --parent` and calls `kill_process_group`
on each one.

If `clickhouse-test` or its parent `fast_test.py` is killed with **SIGKILL**
(e.g. OOM), those handlers never run.  The test subprocesses are re-parented
to `init`/`launchd` and keep running.  Because they are in separate sessions,
`pgrep --parent` no longer finds them.

On Linux the Docker container boundary kills everything when the container
exits.  On macOS Darwin CI (no Docker, no cgroups) the orphans accumulate
across test runs.

---

## Solution: PGID tracking via a group pid file

The kernel stores the PGID directly in the process descriptor.  It is **never
reset** when a process is re-parented.  Therefore `kill_process_group(pgid)`
reaches an orphan as long as we know its PGID — no parent-chain walk needed.

### Group pid file

`_GROUP_PID_FILE` = `{repo}/ci/tmp/clickhouse_test_group_pid`

One PGID per line.  Written and read with `fcntl.LOCK_EX` so parallel test
workers do not corrupt each other's entries.

### Per-test bookkeeping

```python
proc = Popen(command, shell=True, start_new_session=True, preexec_fn=cgroup_fn)
_track_pgid(proc.pid)   # proc.pid == PGID after start_new_session=True
```

```python
# in process_result_impl, after the test exits or is killed:
_untrack_pgid(proc.pid)
```

On a clean run every started test calls `_untrack_pgid`, so the file is empty
(or absent) when `clickhouse-test` exits.  If `clickhouse-test` is SIGKILL'd,
the file retains the PGIDs of tests that were still running at that moment.

### `--cleanup` mode

```python
clickhouse-test --cleanup
```

Calls `cleanup_test_groups()`, which reads the group pid file and calls the
existing `kill_process_group(pgid, None)` on each entry, then removes the file.

### `clickhouse-test` startup

```python
# before (original)
os.setpgid(0, 0)   # new process group, same session — isolates from terminal signals

# temporary intermediate version (no longer used)
os.setsid()        # new session — was needed only for session-based orphan tracking

# current
if os.getpid() != os.getpgid(0):
    os.setpgid(0, 0)   # same as original; setsid is not needed with PGID tracking
```

### Caller cleanup (`run_test` in `clickhouse_proc.py`)

```python
# in finally block after process.wait():
subprocess.run(["clickhouse-test", "--cleanup"], check=False)
```

### Pre-hook and post-hook guard

`ci/defs/job_configs.py` registers the hook for both pre- and post-execution:

```python
darwin_fast_test_jobs = Job.Config(
    ...
    pre_hooks=["python3 ./ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py"],
    post_hooks=["python3 ./ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py"],
)
```

The **pre-hook** cleans up any orphans left by a previous run (e.g. if the
runner was rebooted mid-job and the post-hook never fired).  The **post-hook**
covers the case where `fast_test.py` itself is SIGKILL'd and the `finally`
block in `run_test` never executes.

The hook contains no kill logic of its own — it just calls
`clickhouse-test --cleanup`, the single source of truth for orphan cleanup.

### Cleanup layers

| Layer | Trigger | Mechanism |
|---|---|---|
| `cleanup_child_processes` | SIGTERM/SIGINT/SIGHUP to `clickhouse-test` | `killpg` on each direct child's PGID |
| `run_test()` `finally` | Any exit of `clickhouse-test` (incl. SIGKILL) | `clickhouse-test --cleanup` → `kill_process_group` per PGID in file |
| Pre-hook | Job start (cleans up previous run's orphans) | same — `clickhouse-test --cleanup` |
| Post-hook | Any exit of `fast_test.py` (incl. SIGKILL) | same — `clickhouse-test --cleanup` |

### Remaining limitation

If `runner.py` itself is killed before the post-hook executes, nothing cleans
up.  On a dedicated macOS runner this requires a machine-level failure; a reboot
clears all processes.  For Linux production CI the Docker boundary already covers this.
