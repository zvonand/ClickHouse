# ClickHouse Coverage Build Investigation

## Original Task

Understand the ClickHouse CI coverage pipeline:
1. Find the workflow that builds a coverage build and runs functional tests with coverage instrumentation
2. Understand how metrics/traces are collected after each test and stored in a ClickHouse table
3. Understand how this data is used for test selection
4. Analyze the quality of symbol collection — are symbols correct? Are they deduplicated? What else can be improved?

---

## ⚠️ KEY INSIGHT: DO NOT THINK ABOUT PCs

The `coverage` column in `system.coverage_log` stores runtime PC addresses. These are fragile and NOT what the final test selection pipeline uses. The pipeline uses **symbols** (the `symbol` column), not PCs.

The CIDB export does `arrayJoin(symbol)` — it reads symbol names directly. **Focus on symbol quality and symbol matching, not PC quality.**

---

## Coverage Pipeline Overview

### Build

- **Workflow file**: `ci/workflows/nightly_coverage.py`
- **Runs**: daily at 02:13 UTC
- **CMake flag**: `-DSANITIZE_COVERAGE=1` (NOT the same as LLVM `--coverage`/profraw)
- **Build jobs**: `JobConfigs.coverage_build_jobs[0]` (AMD x86_64)
- **Artifact**: `self-extracting/clickhouse` — ZSTD-compressed unstripped binary; decompresses to itself on first run via `execve`
- **NOT stripped**: binary has full `.symtab` (~47 MB, 1.2M+ symbols) and `.debug_info` (~2 GB); `readelf -SW` confirmed

### What SANITIZE_COVERAGE Does

Uses LLVM's SanitizerCoverage (`-fsanitize-coverage=trace-pc-guard,pc-table`). At each instrumented basic block, the compiler inserts a call to `__sanitizer_cov_trace_pc_guard(guard_ptr)`. ClickHouse implements this callback in `base/base/coverage.cpp`.

The callback stores `__builtin_return_address(0)` (the return address in the caller — the address just after the call to the guard inside the instrumented basic block) into two arrays:
- `current_coverage_array[guard_index]` — reset between tests via `SYSTEM RESET COVERAGE`
- `cumulative_coverage_array[guard_index]` — never reset, accumulates since process startup

### Test Execution

- **Job**: Stateless tests with `amd_coverage` binary, run sequentially (not parallel), 8 batches
- **Key file**: `ci/jobs/functional_tests.py`
- **Detection**: `if "coverage" in to: is_coverage = True`
- **Runner**: `tests/clickhouse-test --collect-per-test-coverage` (defaults to `True`; only fires when `BuildFlags.SANITIZE_COVERAGE in args.build_flags`)
- **JIT settings**: forced to 0 for `SANITIZE_COVERAGE` builds in `SettingsRandomizer.get_random_settings()` — see Bug 2 below

### Per-Test Coverage Collection (in `tests/clickhouse-test`)

All coverage INSERTs are sent via HTTP (`clickhouse_execute_http` → port 8123), not via `clickhouse client`.

After each test completes, TWO rows are inserted per test:

**Row 1 — Server-side INSERT** (`test_name = '{test_name}'`):
```sql
INSERT INTO system.coverage_log
WITH arrayDistinct(arrayFilter(x -> x != 0, coverageCurrent())) AS coverage_distinct
SELECT DISTINCT now(), '{test_name}', coverage_distinct,
       arrayMap(x -> demangle(addressToSymbol(x)), coverage_distinct)
```
Captures which server code paths were exercised since the last `SYSTEM RESET COVERAGE`. This is per-test incremental coverage of server-side execution (parsing, planning, executing queries).

**Row 2 — Client dump INSERT** (`test_name = '{test_name}__client'`):
The `clickhouse client` binary writes its own `cumulative_coverage_array` to `coverage.{database}.{pid}` on exit (via `dumpCoverage()` in `programs/main.cpp`). The test runner reads these files and inserts them:
```sql
INSERT INTO system.coverage_log SETTINGS async_insert=1,...
WITH arrayDistinct(groupArray(data)) AS coverage_distinct
SELECT now(), '{test_name}__client', coverage_distinct,
       arrayMap(x -> demangle(addressToSymbol(x)), coverage_distinct)
FROM input('data UInt64') FORMAT RowBinary
```
Captures cumulative client-side coverage since the client process started. Contains mostly startup/initialization noise — see analysis below.

**Before tests start — baseline INSERT** (`test_name = ''`): Captures coverage accumulated during server startup.

Between tests: `SYSTEM RESET COVERAGE` zeroes `current_coverage_array` and resets all guards to 1.

### Two Row Types: Server vs Client — Key Differences

For `00001_select_1.sql`:

| Row | PCs | DB:: symbols | What it captures |
|-----|-----|-------------|-----------------|
| `00001_select_1.sql` (server) | ~22K | ~20K (query execution) | Code the **server** ran to process this test's queries |
| `00001_select_1.sql__client` (client) | ~62K | ~48K (mostly init) | Cumulative client code including **all startup initialization** |

Client symbols are dominated by startup noise (e.g. `AggregateFunctionCombinator*::getName()` — runs on every client start regardless of which test runs). These appear in ALL tests and are filtered at the inverted index query level (see `find_tests.py` `HAVING` filter).

### Coverage Log Table Schema

```sql
CREATE TABLE IF NOT EXISTS system.coverage_log
(
    time DateTime,
    test_name String,    -- '{test}' for server row, '{test}__client' for client dump
    coverage Array(UInt64),   -- file offsets (client dump) or runtime VAs (server)
    symbol Array(String)      -- demangled symbol names (what actually matters)
) ENGINE = MergeTree ORDER BY test_name
```

### Export to CIDB

**File**: `ci/jobs/scripts/functional_tests/export_coverage.py`

`CoverageExporter.do()` uses `clickhouse local` to read `system.coverage_log` from the on-disk data directory and inserts into CIDB:

```sql
INSERT INTO FUNCTION remoteSecure('{cidb_url}', 'default.checks_coverage_inverted', ...)
SELECT DISTINCT
    arrayJoin(symbol) AS symbol,
    '{check_start_time}' AS check_start_time,
    '{job_name}' AS check_name,
    test_name
FROM system.coverage_log
```

Both server and `__client` rows are exported. Client startup symbols (common to ALL tests) are filtered at query time via `HAVING count(distinct test_name) < total_tests`.

### Test Selection

**Files**: `ci/jobs/scripts/find_tests.py`, `ci/jobs/scripts/find_symbols.py`

```
PR diff (GitHub)
    ↓ parse changed (file, line) pairs for C/C++ files
    ↓ find_symbols.py: DWARF query → symbol names
    ↓ find_tests.py: CIDB query on checks_coverage_inverted
    ↓ selected tests
```

---

## Key Source Files

| File | Purpose |
|------|---------|
| `base/base/coverage.cpp` | Core: `__sanitizer_cov_trace_pc_guard`, `__sanitizer_cov_pcs_init`, `resetCoverage`, `getCoverage*` |
| `base/base/coverage.h` | Declares `getCurrentCoverage`, `getCumulativeCoverage`, `getAllInstrumentedAddresses`, `resetCoverage` |
| `src/Functions/coverage.cpp` | SQL functions `coverageCurrent()`, `coverageCumulative()`, `coverageAll()` |
| `src/Functions/addressToSymbol.cpp` | SQL function `addressToSymbol(UInt64) -> String` |
| `src/Common/SymbolIndex.cpp` | `SymbolIndex::findSymbol` — resolves addresses/offsets to symbol names |
| `src/Common/Coverage.cpp` | `dumpCoverage()` — writes cumulative coverage as **file offsets** on client exit |
| `src/Processors/Formats/Impl/DWARFBlockInputFormat.cpp` | DWARF format reader — exposes ELF DWARF as SQL table |
| `tests/clickhouse-test` | Test runner; per-test INSERT (server + client), baseline INSERT, JIT suppression |
| `ci/workflows/nightly_coverage.py` | NightlyCoverage CI workflow |
| `ci/jobs/functional_tests.py` | Coverage-specific test runner setup |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Exports `coverage_log` to CIDB |
| `ci/jobs/scripts/find_tests.py` | Queries CIDB to find tests relevant to changed symbols |
| `ci/jobs/scripts/find_symbols.py` | Maps changed `(file, line)` pairs to symbol names via DWARF |

---

## How to Run Locally

```bash
mkdir -p tmp/coverage_run/{data,tmp,logs,user_files,format_schemas}
fuser -k 9000/tcp 8123/tcp 9010/tcp 2>/dev/null || true

./clickhouse server --config-file=tmp/coverage_run/config.xml \
    > tmp/coverage_run/logs/server.log 2>&1 &
sleep 3
./clickhouse client --port 9000 --query "SELECT 1"

# Run a test with coverage
python3 tests/clickhouse-test \
    --binary ./clickhouse \
    --client-option "port=9000" \
    --collect-per-test-coverage \
    "00001_select_1"
```

Config (`tmp/coverage_run/config.xml`) must have `allow_introspection_functions=1`. Do NOT use `--config-file=/dev/null` (Poco rejects empty files). Do NOT set `<tcp_port_secure>0</tcp_port_secure>` (triggers SSL setup).

```sql
-- Check results
SELECT test_name, length(coverage) as pcs, countEqual(symbol,'') as empty_syms,
       length(symbol) - countEqual(symbol,'') as good_syms
FROM system.coverage_log ORDER BY time;
```

---

## Bugs Found and Fixed

### Bug 1: Client dump writes runtime VAs — server can't resolve them (FIXED)

**Symptom**: Client dump row (`{test}__client`) had ~62K PCs all with empty symbols.

**Root cause**: `dumpCoverage()` in `src/Common/Coverage.cpp` wrote raw runtime virtual addresses from `cumulative_coverage_array`. The `clickhouse client` process runs at a different ASLR base than the server (PIE binary, ASLR randomizes each process independently). When the server inserts these addresses and calls `addressToSymbol()`:
- `SymbolIndex::findObject(client_addr)` returns null — client's ASLR base is different from server's
- Fallback path: tries `client_addr` as a raw file offset — but `0x5573...` (runtime VA) is 94 TB as a file offset → no symbol → empty

**Fix** (`src/Common/Coverage.cpp`): subtract the binary's load base before writing to the dump file:
```cpp
uintptr_t load_base = 0;
if (const DB::SymbolIndex::Object * self = DB::SymbolIndex::instance().thisObject())
    load_base = reinterpret_cast<uintptr_t>(self->address_begin);
// ...
data.push_back(addr - load_base);  // store file offset, not runtime VA
```
Now the dump contains file offsets (e.g. `0x1AE6...` = ~450 MB into the binary, valid text section offset). `SymbolIndex::findSymbol()` resolves them correctly via the same fallback path used by `system.stack_trace` (after PR #82809).

**Result**: Client dump row now has 0 empty symbols.

### Bug 2: JIT settings in random session settings contaminate server coverage (FIXED)

**Symptom**: Server-side `coverageCurrent()` row had all empty symbols when random settings happened to enable JIT.

**Root cause**: `clickhouse-test` randomizes session settings per test including `compile_expressions=1`, `compile_sort_description=1`, `compile_aggregate_expressions=1`. These are sent as session settings to the server. The server JIT-compiles functions at runtime into anonymous mmap memory. JIT code calls instrumented C++ functions; `__builtin_return_address(0)` inside `__sanitizer_cov_trace_pc_guard` returns a JIT address (anonymous mmap, not in the binary's text segment). `SymbolIndex::findObject` returns null, fallback fails (JIT address is too large as file offset) → empty symbol.

**Fix** (`tests/clickhouse-test` `SettingsRandomizer.get_random_settings()`): force all 6 JIT settings to 0 for `SANITIZE_COVERAGE` builds:
```python
if is_coverage:
    for jit_setting in (
        "compile_expressions", "compile_aggregate_expressions", "compile_sort_description",
        "min_count_to_compile_expression", "min_count_to_compile_aggregate_expression",
        "min_count_to_compile_sort_description",
    ):
        random_settings[jit_setting] = 0
```
All other random settings still apply. Mirrors existing pattern for `BuildFlags.DEBUG`.

### Bug 3: `BUILD_STRIPPED_BINARY=1` was useless in AMD_COVERAGE (FIXED)

`ci/jobs/build_clickhouse.py` had `-DBUILD_STRIPPED_BINARY=1` in the AMD_COVERAGE cmake command. This creates `clickhouse-stripped` which is never uploaded or used for coverage (only used by `build_master_head_hook.py` for release builds). Wasted time stripping a 6 GB binary. Removed.

### Minor: `coverageAll()` misparses PC table

`__sanitizer_cov_pcs_init` receives `(PC, PCFlags)` pairs but `base/base/coverage.cpp` treats them as a flat array. Every other entry is a flag (0 or 1). `coverageAll()` returns ~50% garbage. Not critical since `coverageAll()` is not exported to CIDB.

---

## `SymbolIndex::findSymbol` — How Address Resolution Works

Located in `src/Common/SymbolIndex.cpp`:

```
findSymbol(addr):
    object = findObject(addr)            -- binary search over mapped objects [address_begin, address_end)
    if object found:
        offset = addr - object.address_begin   -- convert runtime VA → file offset
    else:
        offset = addr                    -- fallback: treat directly as raw file offset
    return find(offset, symbols)         -- binary search over .symtab entries by file offset
```

`address_begin = info->addr` (load base from `dl_iterate_phdr`), `address_end = info->addr + elf_file_size`.

Symbols are stored as **file offsets** (pre-link virtual addresses from ELF `.symtab`). The fallback path (treat value as file offset) is what makes both `system.stack_trace` and fixed client dump addresses resolve correctly.

---

## Detailed Test Selection Pipeline

### What a Symbol Is

A **demangled C++ function name** from the ELF symbol table, e.g.:
```
DB::MergeTreeData::checkPartDynamicColumns(DB::Block&, std::string&) const
```
It is the enclosing function of the changed code — NOT a file+line.

### Full Pipeline: Changed Line → Tests

**Step 1** — `find_symbols.py`: fetch PR diff from GitHub, parse `(file, line)` for all added/removed lines in C/C++ files.

**Step 2** — `find_symbols.py`: DWARF query via `clickhouse local`:
```sql
SELECT diff.filename, diff.line, binary.address, binary.linkage_name,
    if(empty(binary.linkage_name),
        demangle(addressToSymbol(binary.address)),
        demangle(binary.linkage_name)) AS symbol
FROM file('stdin', 'CSVWithNames', ...) AS diff
ASOF LEFT JOIN (
    SELECT decl_file, decl_line, linkage_name, ranges[1].1 AS address
    FROM file('{clickhouse_binary}', 'DWARF')
    WHERE tag = 'subprogram' AND (notEmpty(linkage_name) OR address != 0)
      AND notEmpty(decl_file)
) AS binary
ON basename(diff.filename) = basename(binary.decl_file)
AND diff.line >= binary.decl_line
```

**Step 3** — `find_tests.py`: one CIDB query per symbol:
```sql
SELECT groupArray(test_name) AS tests
FROM checks_coverage_inverted
WHERE check_start_time > now() - interval 3 days
  AND check_name LIKE 'Stateless%'
  AND symbol = '{SYMBOL}'
HAVING count(distinct test_name) < {TOTAL_TESTS}
```
`TOTAL_TESTS` is precomputed once before the loop. The `HAVING` filter excludes symbols appearing in ALL tests (client startup noise).

**Step 4** — Python-side filter: symbols with >100 tests are skipped ("too common code"). Union with: directly changed test files, previously failed tests.

### DWARF Format Schema

The `file(path, 'DWARF')` table format exposes:

| Column | Type | Description |
|--------|------|-------------|
| `tag` | String | DWARF DIE tag (`subprogram`, `inlined_subroutine`, etc.) |
| `name` | String | Human-readable name |
| `linkage_name` | String | Mangled C++ name |
| `decl_file` | LowCardinality(String) | Source file of declaration |
| `decl_line` | UInt32 | Source line of declaration (function start) |
| `ranges` | Array(Tuple(UInt64, UInt64)) | Machine code address ranges: **(start, end)** pairs |

`ranges[1].1` = start address, `ranges[1].2` = end address of the function's machine code.

---

## Problems with Current DWARF→Symbol Matching (Not Yet Fixed)

### Problem 1: Wrong canonical symbol name

Current uses `demangle(linkage_name)`. Should use `demangle(addressToSymbol(address))` when `address != 0` — that's exactly what coverage stored. For inlined functions (`address=0`), no ELF symbol exists so no match is possible regardless.

**Fix**: `if(binary.address != 0, demangle(addressToSymbol(binary.address)), demangle(binary.linkage_name))`

### Problem 2: No upper bound on function line range

ASOF JOIN only has lower bound (`decl_line ≤ changed_line`). Lines between two functions are wrongly assigned to the preceding one.

**Fix**: use `ranges[1].2` (end address) + `addressToLine(ranges[1].2 - 1)` to get `end_line`, add `diff.line <= end_line`. Filter `ranges[1].1 != 0` to drop abstract inlines.

### Problem 3: Template instantiations — only one found

All instantiations share the same `decl_line`. ASOF JOIN picks one arbitrarily. Tests exercising `foo<String>` are missed when `foo<int>` was picked.

**Fix**: strip template args → use `startsWith(symbol, base_name + '<')` in CIDB query.

### Problem 4: Basename path matching — false positives

`basename(...)` — same-named files in different directories collide.

**Fix**: use last 2 path components: `arrayStringConcat(arraySlice(splitByChar('/', path), -2), '/')`.

### Problem 5: N+1 CIDB queries

One round-trip per symbol. Fix: batch with `symbol IN (...)`.

### Unfixable: inlined functions in headers

`address=0` in DWARF → no ELF symbol → can never match coverage data. Only fixable by switching to line-level coverage.

---

## CIDB Tables

| Table | Schema | Purpose |
|-------|--------|---------|
| `default.checks_coverage_inverted` | `(symbol, check_start_time, check_name, test_name)` | Inverted index: symbol → tests that cover it |
| `coverage_ci.coverage_data` | metrics | Coverage metrics |

---

## Summary of All Changes Made

| File | Change | Why |
|------|--------|-----|
| `src/Common/Coverage.cpp` | `dumpCoverage()` subtracts load base → writes file offsets | Client's runtime VAs are unresolvable by server with different ASLR base |
| `tests/clickhouse-test` | Force 6 JIT settings to 0 in `get_random_settings()` for SANITIZE_COVERAGE | JIT addresses in anonymous mmap → unresolvable → empty symbols in server row |
| `tests/clickhouse-test` | Server row named `{test}`, client row named `{test}__client` | Distinguish the two row types clearly |
| `ci/jobs/build_clickhouse.py` | Remove `-DBUILD_STRIPPED_BINARY=1` from AMD_COVERAGE | Creates unused stripped binary; wastes build time |
| `ci/jobs/scripts/find_tests.py` | `HAVING count(distinct test_name) < {TOTAL_TESTS}` + precompute total | Filter client startup symbols (in all tests) from inverted index queries |

---

## Use Cases for Coverage Data

1. **Targeted test selection** (current) — run only tests covering changed symbols
2. **Test ordering** — run tests covering most recently changed code first for early CI signal
3. **Minimum covering set** — smallest subset covering all symbols (smoke suite)
4. **Batch composition** — maximize coverage diversity per CI batch
5. **Dead code detection** — symbols in `coverageAll()` never in `checks_coverage_inverted`
6. **Coverage % per subsystem** — `DB::MergeTree*` vs `DB::Aggregate*` etc.
7. **Coverage regression** — symbols dropped between nightly builds → behavioral change
8. **Test gap filing** — find tests closest to uncovered symbols
9. **Redundant test detection** — tests whose symbol set is a strict subset of another's
10. **Test uniqueness score** — fraction of a test's symbols covered by no other test
11. **Flakiness correlation** — more symbols → more non-determinism → flakier
12. **Test naming quality** — tests covering symbols far outside apparent domain
13. **Change blast radius** — changed symbols × test count → PR risk score
14. **Coupling detection** — symbols always co-appearing in the same tests
15. **Refactoring safety** — how many tests cover a function before renaming
16. **Root cause narrowing** — failing test ∩ changed symbols → exact functions exercised
17. **Symbol ownership** — join with `git blame` on symbol definition file
18. **Coverage trend** — `count(distinct symbol)` per week vs code growth
19. **High-value test identification** — tests that are sole coverer of many symbols
20. **Production exception cross-check** — stack trace symbols with 0 tests = direct gap
