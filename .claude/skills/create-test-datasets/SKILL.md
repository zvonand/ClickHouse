---
name: create-test-datasets
description: Create test datasets (hits, visits, tpcds, tpch) from standard scripts. Ensures the server is running first.
argument-hint: [hits] [visits] [tpcds] [tpch]
disable-model-invocation: false
allowed-tools: Bash(clickhouse-client:*), Bash(clickhouse:*), Bash(pgrep:*), Bash(ls:*), Bash(cat:*), Bash(bash:*), Read, Glob, Grep, AskUserQuestion
---

# Create Test Datasets Skill

Set up test datasets by executing standard creation scripts. Supports `hits`, `visits`, `tpcds`, and `tpch`.

## Arguments

- `$ARGS` (optional): Space-separated list of dataset names from `{hits, visits, tpcds, tpch}`. If no arguments are provided, defaults to `hits visits`.

Examples:
- `/create-test-datasets` — sets up `hits` and `visits` (default)
- `/create-test-datasets hits` — sets up only `hits`
- `/create-test-datasets tpch tpcds` — sets up TPC-H and TPC-DS
- `/create-test-datasets hits visits tpch` — sets up all three

## Process

### 1. Parse arguments

Parse the argument string into a set of requested datasets. Valid values are `hits`, `visits`, `tpcds`, `tpch`. If no arguments are provided, use `{hits, visits}` as the default. If any argument is not in the valid set, report an error and stop.

### 2. Verify the server is running

```bash
clickhouse-client -q "SELECT 1" 2>&1
```

If the server is not reachable, report an error and stop. Do **not** attempt to start the server — ask the user to start it first.

### 3. Set up hits and/or visits

Only execute this step if `hits` or `visits` (or both) are in the requested set.

Run the full `create.sql` script:
```bash
clickhouse-client --multiquery < tests/docker_scripts/create.sql
```

This creates `datasets.hits_v1` and `datasets.visits_v1`.

Then create the `test` database and rename only the requested tables:
```bash
clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test"
```

If `hits` is requested:
```bash
clickhouse-client -q "RENAME TABLE datasets.hits_v1 TO test.hits"
```

If `visits` is requested:
```bash
clickhouse-client -q "RENAME TABLE datasets.visits_v1 TO test.visits"
```

If only one of `hits`/`visits` was requested, drop the unrequested table to avoid leaving orphans:
- If `hits` requested but not `visits`: `DROP TABLE IF EXISTS datasets.visits_v1`
- If `visits` requested but not `hits`: `DROP TABLE IF EXISTS datasets.hits_v1`

### 4. Set up TPC-DS

Only execute this step if `tpcds` is in the requested set.

```bash
bash tests/docker_scripts/create_tpcds.sh
```

### 5. Set up TPC-H

Only execute this step if `tpch` is in the requested set.

```bash
bash tests/docker_scripts/create_tpch.sh
```

### 6. Verify

For each dataset that was set up, confirm it exists and report row counts:

- If `hits`: `SELECT 'test.hits', count() FROM test.hits`
- If `visits`: `SELECT 'test.visits', count() FROM test.visits`
- If `tpcds`: `SELECT name, total_rows FROM system.tables WHERE database = 'tpcds' ORDER BY name`
- If `tpch`: `SELECT name, total_rows FROM system.tables WHERE database = 'tpch' ORDER BY name`

Report all results to the user.
