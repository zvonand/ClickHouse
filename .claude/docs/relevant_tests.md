# Related Test Selection — Developer Guide

## Overview

The coverage-based related test selector (`ci/jobs/scripts/find_tests.py`) finds
stateless tests likely to catch regressions in a PR by querying a ClickHouse CI
database (CIDB) that records which lines each test covered in recent nightly runs.

---

## Architecture

### Data flow

```
NightlyCoverage CI job
  └─ Build amd_per_test_coverage binary (WITH_COVERAGE=ON -DWITH_COVERAGE_DEPTH=ON)
  └─ Run stateless tests (clickhouse-test --collect-per-test-coverage)
       ├─ SYSTEM SET COVERAGE TEST 'test_name'  (before each test)
       └─ SYSTEM SET COVERAGE TEST ''           (flush + reset counters)
  └─ export_coverage.py (reads local server tables, inserts into CIDB)
       ├─ system.coverage_log         → checks_coverage_lines
       └─ system.coverage_indirect_calls → checks_coverage_indirect_calls
```

### CIDB tables (ClickHouse cluster, read-only via play user)

| Table | Size | Content |
|---|---|---|
| `checks_coverage_lines` | ~400M rows | Per-test file/line coverage — **main table** |
| `checks_coverage_indirect_calls` | 0 rows (new) | Per-test virtual/fn-ptr callee offsets |
| `checks_coverage_inverted` | 12B rows | Legacy symbol→test index (not used by find_tests) |

Schema of `checks_coverage_lines`:
```sql
file LowCardinality(String), line_start UInt32, line_end UInt32,
check_start_time DateTime('UTC'), check_name LowCardinality(String),
test_name LowCardinality(String), min_depth UInt8, branch_flag UInt8
-- ORDER BY (check_start_time, file, line_start, check_name, test_name)
-- PARTITION BY toYYYYMM(check_start_time)
-- Indexes: bloom_filter on file, minmax on line_start
```

Schema of `checks_coverage_indirect_calls`:
```sql
check_start_time DateTime('UTC'), check_name LowCardinality(String),
test_name LowCardinality(String), caller_name_hash UInt64,
caller_func_hash UInt64, callee_offset UInt64, call_count UInt64
-- ORDER BY (check_start_time, check_name, test_name, caller_name_hash)
-- NOTE: should have PARTITION BY and callee_offset in ORDER BY — ask CIDB admin
```

### Accessing CIDB

```python
from ci.praktika.cidb import CIDB
from ci.praktika.settings import Settings

cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
result = cidb.query("SELECT count() FROM checks_coverage_lines WHERE ...", log_level="")
```

Or from shell (read-only play user, no password):
```bash
PYTHONPATH=./ci:. python3 -c "
from ci.praktika.cidb import CIDB
from ci.praktika.settings import Settings
cidb = CIDB(url=Settings.CI_DB_READ_URL, user='play', passwd='')
print(cidb.query('SELECT uniqExact(test_name) FROM checks_coverage_lines WHERE check_start_time > now() - interval 3 days', log_level=''))
"
```

---

## How find_tests.py works

```
get_all_relevant_tests_with_info()
  1. get_changed_tests()          — test files changed in the PR diff (highest priority)
  2. get_previously_failed_tests() — tests that failed in prior CI runs for this PR
  3. get_most_relevant_tests()    — coverage-based (see below)
```

### Coverage-based selection (3 passes)

**Pass 1 — Direct coverage** (`get_tests_by_changed_lines`):
- Gets changed `(file, line_no)` pairs from `gh pr diff`
- Filters to `src/`, `programs/`, `utils/`, `base/` (COVERAGE_TRACKED_PREFIXES)
- CIDB query: find tests covering those lines in last 3 days
- Skips regions where `region_test_count > MAX_TESTS_PER_LINE (200)` — prevents
  broad hits from infrastructure files like `Context.cpp`, `Settings.cpp`
- Returns `(test, region_width, min_depth, region_test_count)` per changed line

**Pass 2 — Sibling file expansion** (`_query_sibling_dir_tests`):
- Extracts domain keywords from changed C++ filenames (CamelCase split, strips common words)
  - e.g. `CHColumnToArrowColumn.cpp` → `["Arrow"]`
  - e.g. `MergeTreeIndexConditionText.cpp` → `["Index", "Text"]`
- Finds sibling files in the same directory that primary tests also cover
- Finds OTHER tests covering those sibling files
- Adds them with `INDIRECT_CALL_WIDTH=3000` (ranked below direct hits)

**Pass 3 — Indirect call co-occurrence** (`_query_indirect_call_tests`):
- Self-joins `checks_coverage_indirect_calls` on `callee_offset`
- Finds tests that call the same virtual/fn-ptr callees as primary tests
- Requires `checks_coverage_indirect_calls` to be populated (new, see below)
- Adds them with `INDIRECT_CALL_WIDTH=3000`

### Scoring (get_most_relevant_tests)
```
score = sum(1.0 / (region_width × region_test_count)) across matched lines
Tier A: narrow (width≤20) AND shallow (min_depth≤3) → direct callers
Tier B: narrow, deep path
Tier C: broad (width>20)
Sibling/Indirect: SIBLING_DIR_WIDTH=10000 → always Tier C, ranked last
```

### Known limitations

| Change type | Result | Why |
|---|---|---|
| `constexpr`/`static const` at declaration | 0 tests | Compile-time, no runtime counter |
| Infrastructure file (Context, Settings) | 0 or sibling only | Capped at 200 tests/region |
| Niche C++ (LDAP, CLI, crash handlers) | 0 tests | Files not covered by stateless suite |
| PR > 30 days old | degraded | CIDB window is 3 days; old coverage data gone |
| Test-file-only PR | changed test files | Detected by `get_changed_tests()` |

---

## Running find_tests locally

```bash
cd /path/to/ClickHouse
# Get related tests for a PR
PYTHONPATH=./ci:. python3 ci/jobs/scripts/find_tests.py <PR_NUMBER>

# Example output:
# [find_tests] querying coverage for 33 changed lines across 1 files
# [find_tests] CIDB query: 0.14s, response=261956 bytes
# [find_tests] sibling-dir query: 0.18s, response=1654 bytes
# [find_tests] done in 0.35s: 26/33 lines matched, 112 unique tests selected
# All selected tests (112):
#  02346_text_index_creation.sql  [narrow, depth=28, tests=8]
#  ...
```

---

## Validating coverage data locally

### Build a minimal coverage binary

```bash
# Configure (disable heavy/unnecessary deps for speed)
cmake -S . -B build_coverage \
    -DCMAKE_BUILD_TYPE=None \
    -DWITH_COVERAGE=ON \
    -DWITH_COVERAGE_DEPTH=ON \
    -DCMAKE_C_COMPILER=clang-21 \
    -DCMAKE_CXX_COMPILER=clang++-21 \
    -DENABLE_TESTS=0 -DENABLE_UTILS=0 \
    -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON \
    -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON \
    -DENABLE_LIBRARIES=1 \
    -DENABLE_AWS_S3=OFF \
    -DENABLE_AZURE_BLOB_STORAGE=OFF \
    -DENABLE_GOOGLE_CLOUD_CPP=OFF \
    -DENABLE_PROMETHEUS_PROTOBUFS=OFF \
    -DENABLE_HDFS=OFF \
    -DENABLE_GRPC=OFF \
    -DENABLE_PARQUET=OFF -DENABLE_ORC=OFF -DENABLE_AVRO=OFF -DENABLE_ARROW=OFF \
    -DENABLE_KAFKA=OFF -DENABLE_NATS=OFF -DENABLE_AMQPCPP=OFF -DENABLE_CAPNP=OFF \
    -DENABLE_EMBEDDED_COMPILER=OFF \
    -DENABLE_RUST=OFF -DENABLE_ISAL_LIBRARY=OFF

# Build (~1-2 hours first time, incremental after)
ninja -C build_coverage clickhouse > build_coverage/build.log 2>&1
```

**Note**: If build fails with `missing source KqlFunctionBase.cpp`, run:
```bash
touch src/Functions/Kusto/CMakeLists.txt && cmake build_coverage
```

### Run local validation

```bash
# Start a fresh server
mkdir -p tmp/cov_test/data tmp/cov_test/logs
build_coverage/programs/clickhouse server -- \
    --path tmp/cov_test/data \
    --logger.log tmp/cov_test/logs/server.log \
    --tcp_port 9041 --http_port 8164 --listen_host 127.0.0.1 &
sleep 5

CH="build_coverage/programs/clickhouse client --port 9041"

# Create coverage tables (matching clickhouse-test schema)
$CH --query "CREATE TABLE IF NOT EXISTS system.coverage_log
(time DateTime, test_name LowCardinality(String), file LowCardinality(String),
 line_start UInt32, line_end UInt32, min_depth UInt8 DEFAULT 255, branch_flag UInt8 DEFAULT 0)
ENGINE = ReplacingMergeTree(time) ORDER BY (test_name, file, line_start, line_end)"

$CH --query "CREATE TABLE IF NOT EXISTS system.coverage_indirect_calls
(test_name LowCardinality(String), callee_offset UInt64,
 caller_name_hash UInt64, caller_func_hash UInt64, call_count UInt64)
ENGINE = ReplacingMergeTree(call_count) ORDER BY (test_name, callee_offset)"

# Run two tests and verify per-test isolation
$CH --query "SYSTEM SET COVERAGE TEST 'test_A'"
$CH --query "SELECT sin(number), arraySort([3,1,2]) FROM numbers(100) LIMIT 5"
$CH --query "SYSTEM SET COVERAGE TEST 'test_B'"
$CH --query "SELECT lower('Hello'), upper('World')"
$CH --query "SYSTEM SET COVERAGE TEST ''"
sleep 1

# Verify coverage_log populated
$CH --query "SELECT count(), uniqExact(file) FROM system.coverage_log WHERE test_name='test_A'"
# Expected: ~3000-7000 rows, ~100-300 files

# Verify indirect calls isolated (shared < total_A proves no accumulation)
$CH --query "SELECT count(), uniqExact(callee_offset) FROM system.coverage_indirect_calls WHERE test_name='test_A'"
$CH --query "SELECT count() FROM (
    SELECT callee_offset FROM system.coverage_indirect_calls WHERE test_name='test_A'
    INTERSECT
    SELECT callee_offset FROM system.coverage_indirect_calls WHERE test_name='test_B'
)"
# shared_count < test_A_unique_count → per-test reset is working

# Verify FINAL works (ReplacingMergeTree)
$CH --query "SELECT count() FROM system.coverage_indirect_calls FINAL"

# Verify clickhouse-local can read it (simulates export)
kill $(lsof -ti tcp:9041) 2>/dev/null; sleep 2
build_coverage/programs/clickhouse local \
    --path tmp/cov_test/data --only-system-tables \
    --query "SELECT count(), uniqExact(callee_offset) FROM system.coverage_indirect_calls FINAL" \
    -- --zookeeper.implementation=testkeeper
```

### Check CIDB data quality

```python
# Quick checks to verify coverage data is fresh and healthy
PYTHONPATH=./ci:. python3 - << 'EOF'
from ci.praktika.cidb import CIDB
from ci.praktika.settings import Settings
cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")

# Data freshness
print(cidb.query("""
SELECT check_name, max(toDate(check_start_time)) AS last_run,
       uniqExact(test_name) AS tests, count() AS rows
FROM checks_coverage_lines
WHERE check_start_time > now() - interval 7 days
GROUP BY check_name
ORDER BY last_run DESC
LIMIT 10
""", log_level=""))

# Indirect calls (should be >0 after first fixed NightlyCoverage run)
print(cidb.query("""
SELECT count() AS rows, uniqExact(test_name) AS tests,
       uniqExact(callee_offset) AS unique_callees
FROM checks_coverage_indirect_calls
WHERE check_start_time > now() - interval 3 days
""", log_level=""))
EOF
```

---

## Key files

| File | Purpose |
|---|---|
| `ci/jobs/scripts/find_tests.py` | Main algorithm — `Targeting` class |
| `ci/jobs/functional_tests.py` | Calls `get_all_relevant_tests_with_info()`, passes to clickhouse-test |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Exports local coverage tables to CIDB |
| `base/base/coverage.cpp` | Runtime: reads LLVM profile data, resets per-test, collects indirect calls |
| `src/Common/CoverageCollection.cpp` | Server-side: maps counters to source regions, inserts into system tables |
| `src/Common/LLVMCoverageMapping.cpp` | Parses ELF `__llvm_covmap`/`__llvm_covfun` sections at startup |
| `tests/clickhouse-test` | Creates `system.coverage_log` and `system.coverage_indirect_calls` tables |
| `ci/workflows/nightly_coverage.py` | NightlyCoverage workflow definition |

## NightlyCoverage workflow

Two builds run every night:
- `Build (amd_llvm_coverage)`: `WITH_COVERAGE=ON` only → standard profraw for llvm-cov report
- `Build (amd_llvm_coverage_per_test)`: `WITH_COVERAGE=ON -DWITH_COVERAGE_DEPTH=ON` → per-test coverage

Trigger manually:
```bash
gh workflow run nightly_coverage.yml --ref <branch>
gh run list --workflow=nightly_coverage.yml --limit=5
```

## Indirect call collection details

LLVM value profiling (`-enable-value-profiling=true`) records runtime callee addresses
at each virtual call / function pointer site into `ValueProfNode` linked lists.
**Critical**: `__llvm_profile_reset_counters()` does NOT reset these nodes.
The fix in `base/base/coverage.cpp` zeros `node->count` after reading so each test
starts fresh — without this, every test accumulates all prior tests' indirect calls.

The `callee_offset = callee_address − binary_load_base` is stable across ASLR
restarts for the same binary build. The find_tests self-join query uses this:
```sql
-- Find tests that share virtual callees with primary tests (covering changed files)
SELECT DISTINCT ic2.test_name
FROM checks_coverage_indirect_calls ic1
JOIN checks_coverage_indirect_calls ic2 ON ic1.callee_offset = ic2.callee_offset
WHERE ic1.test_name IN (primary_tests)
  AND ic2.test_name NOT IN (primary_tests)
  AND ic1.callee_offset IN (
      SELECT callee_offset FROM checks_coverage_indirect_calls
      GROUP BY callee_offset HAVING uniqExact(test_name) < 200
  )
LIMIT 200
```
