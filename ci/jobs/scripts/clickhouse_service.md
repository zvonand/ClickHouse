# ClickHouseService

`ClickHouseService` is the successor to the deprecated `ClickHouseProc`. It manages a single
ClickHouse server process and owns all artifact collection needed after a CI job completes.

## Server lifecycle

The class is a context manager. The server starts on `__enter__` and is gracefully terminated
on `__exit__`:

```python
with ClickHouseService() as ch:
    # server is ready here
    ...
# server is stopped, artifacts can still be collected
files = ch.prepare_logs()
```

On entry the class downloads the ClickHouse binary if absent, creates the standard symlinks
(`clickhouse-server`, `clickhouse-client`, `clickhouse-local`), copies the default server
config if it does not yet exist, then starts the process and waits until `SELECT 1` succeeds.

## Artifact collection

| Method | What it collects |
|---|---|
| `collect_logs()` | All `*.log` files from the server log directory |
| `collect_cores(directory)` | Up to 3 `core.*` files: compressed with zstd, then encrypted (AES-256-CBC key wrapped with RSA-OAEP using `ci/defs/public.pem`) |
| `collect_jemalloc_profiles()` | Latest jemalloc heap profile per PID, rendered as text and SVG flamegraph, archived as `jemalloc.tar.zst` |
| `collect_coordination_logs()` | Keeper coordination directory archived as `coordination.tar.gz` |
| `prepare_logs()` | Calls all four collectors above and returns the combined file list |

`collect_cores` asserts that the RSA public key exists before proceeding.

## Relation to ClickHouseProc

`ClickHouseProc` is deprecated. Its `collect_and_encrypt_cores` function and
`_collect_core_dumps` method have been removed; `ClickHouseProc.prepare_logs` now delegates
core collection to `ClickHouseService.collect_cores`.

New jobs should use `ClickHouseService` directly. `ast_fuzzer_job` has already been migrated.
