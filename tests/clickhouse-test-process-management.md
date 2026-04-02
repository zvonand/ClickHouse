# clickhouse-test process management

## Problem: orphan processes when the runner dies unexpectedly

### Original design

`clickhouse-test` moved itself into a new process group on startup:

```python
os.setpgid(0, 0)   # new process group, same session
```

Each `.sh` test was spawned in a completely new **session**:

```python
Popen(command, shell=True, start_new_session=True, ...)
```

`start_new_session=True` is equivalent to calling `setsid()` in the child before `exec`. The test process becomes a session leader in a brand-new session, with no relationship to any other process group or session.

### Why `start_new_session=True` was used

When bash runs a pipeline, it does not forward signals to its children by default. If you send SIGTERM to a bash process, the children (ClickHouse client, grep, awk, …) survive. `os.killpg(pgid, SIGKILL)` solves this: it kills every process in the group simultaneously, regardless of signal forwarding. For that to work, each test's subprocess tree must be in a known, dedicated process group — hence `start_new_session=True` (or equivalently `preexec_fn=os.setpgrp`).

### The orphan gap

`cleanup_child_processes` in `clickhouse-test` uses `pgrep --parent {pid}` to find direct children, then calls `os.killpg` on each child's process group. This works when `clickhouse-test` is alive and its signal handlers run (SIGTERM, SIGINT, SIGHUP).

If `clickhouse-test` is killed with **SIGKILL** or dies due to an OOM event, its signal handlers never run. The `.sh` test subprocesses are re-parented to `init`/`launchd`, but they keep running. Because they are in separate sessions, they are unreachable via the parent's process group. No kernel mechanism automatically reaps them.

This was not a problem when running inside Docker: the container's PID namespace acts as a hard boundary — when the container exits, every process inside dies regardless of sessions or process groups.

On macOS (no Docker, no cgroups, no `prctl(PR_SET_PDEATHSIG)`), orphans accumulate across test runs.

### Secondary gap: nested sessions via `timeout`

GNU `timeout` calls `setsid()` on the child it wraps, creating a grandchild session. `cleanup_child_processes` only walks one level deep (`pgrep --parent`), so grandchildren in nested sessions can survive a graceful cleanup too. In practice this is bounded: `timeout` will eventually kill its child when the timer fires, even if the parent is gone.

---

## Solution: session-based containment

The fix replaces `start_new_session=True` with `preexec_fn=os.setpgrp` for `.sh` test subprocesses, and changes `clickhouse-test`'s own startup call from `os.setpgid(0, 0)` to `os.setsid()`.

### `clickhouse-test` startup

```python
# before
os.setpgid(0, 0)   # new process group, same session as caller

# after
os.setsid()        # new session (and new process group); session ID == our PID
```

`os.setsid()` creates a new session whose ID equals `clickhouse-test`'s PID. Every process spawned afterwards that does not call `setsid()` itself inherits this session.

### `.sh` test spawning

```python
# before
Popen(command, shell=True, start_new_session=True, preexec_fn=cgroup_fn)

# after
def _preexec_with_setpgrp():
    os.setpgrp()          # new process group — os.killpg still works per-test
    if cgroup_fn:
        cgroup_fn()

Popen(command, shell=True, preexec_fn=_preexec_with_setpgrp)
```

`os.setpgrp()` gives each test its own process group (preserving the per-test `os.killpg` kill that already existed), but keeps it in the same session as `clickhouse-test`. Session membership survives re-parenting: even if `clickhouse-test` dies and the bash process is re-parented to `launchd`, the session ID field in the process table does not change.

### Caller cleanup (`run_test` in `clickhouse_proc.py`)

After `process.wait()` returns — regardless of whether `clickhouse-test` exited normally, timed out, or was killed — the caller performs a session sweep:

```python
session_id = process.pid   # equals clickhouse-test's setsid() session

# in finally:
result = subprocess.run(["ps", "-A", "-o", "pid=,sess="], ...)
for pid, sess in parsed_rows:
    if sess == session_id:
        os.kill(pid, signal.SIGKILL)
```

`ps -A -o pid,sess` works on both Linux and macOS.

### Post-hook guard: `fast_test.py` killed with SIGKILL

The `finally` block in `run_test` is Python-level and does not run if `fast_test.py` itself is killed with SIGKILL or OOM-killed.  To cover this case, `run_test` writes the session ID to a file **before** calling `process.wait()`:

```python
# written immediately after Popen(), before anything can block
ClickHouseProc.SESSION_ID_FILE.write_text(str(session_id))   # ci/tmp/clickhouse_test_session_id
```

The file is deleted in the `finally` block on any normal or handled exit.  If `fast_test.py` is killed before that, the file persists.

A post-hook registered on the `darwin_fast_test_jobs` job definition runs after the job script exits regardless of how it died.  It reads the file and kills the session:

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

### Cleanup layers, innermost to outermost

| Layer | Trigger | Kills |
|---|---|---|
| `clickhouse-test` `cleanup_child_processes` | SIGTERM/SIGINT/SIGHUP to the runner | `.sh` test process groups, one level deep |
| `run_test()` `finally` block | Any exit of `clickhouse-test` (incl. SIGKILL) | Entire session via `ps -A -o pid,sess` |
| Post-hook `clickhouse_test_cleanup_hook.py` | Any exit of `fast_test.py` (incl. SIGKILL) | Same session, read from the session ID file |

### What each failure mode now looks like

| Scenario | Before | After |
|---|---|---|
| Normal exit | Cleaned up by `clickhouse-test` signal handlers | Same + session sweep mops up any stragglers |
| `clickhouse-test` SIGKILL / OOM | Orphans survive indefinitely | `run_test()` `finally` kills by session |
| `fast_test.py` SIGKILL / OOM | Orphans survive indefinitely | Post-hook reads session ID file, kills by session |
| Runner process (`runner.py`) killed | Orphans survive indefinitely | Not covered — would need an external watchdog or cgroup |
| GNU `timeout` grandchildren | Killed by `timeout` on expiry | Same (unchanged, bounded by timeout) |

### Remaining limitation

If the runner process (`runner.py`) itself is killed before the post-hook executes, nothing cleans up.  On a dedicated macOS CI runner this is an extreme failure mode (full machine OOM or kernel panic); a reboot clears all processes anyway.  For Linux production CI the existing Docker/cgroup boundary already covers this.
