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

The callback stores `__builtin_return_address(0)` (the return address in the caller) into two arrays:
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

**Row 2 — Client dump INSERT** (`test_name = '{test_name}__client'`):
The `clickhouse client` binary writes its `cumulative_coverage_array` to `coverage.{database}.{pid}` on exit. The test runner reads these files and inserts them. These are dominated by startup/init symbols — see analysis below.

**Before tests start — baseline INSERT** (`test_name = ''`): captures server startup coverage.

Between tests: `SYSTEM RESET COVERAGE` zeroes `current_coverage_array` and resets guards to 1.

### Coverage Log Table Schema

```sql
CREATE TABLE IF NOT EXISTS system.coverage_log
(
    time DateTime,
    test_name String,    -- '{test}' for server row, '{test}__client' for client dump
    coverage Array(UInt64),
    symbol Array(String)
) ENGINE = MergeTree ORDER BY test_name
```

### Export to CIDB

**File**: `ci/jobs/scripts/functional_tests/export_coverage.py`

Exports to `default.checks_coverage_inverted`:
- Exports ALL symbols (no namespace filter — see Symbol Normalization section)
- Excludes `__client` rows: `WHERE NOT endsWith(test_name, '__client')`
- Normalizes symbols (arg list stripping, trailing template stripping)

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
| `base/base/coverage.cpp` | Core: `__sanitizer_cov_trace_pc_guard`, `resetCoverage`, `getCoverage*` |
| `src/Functions/coverage.cpp` | SQL functions `coverageCurrent()`, `coverageCumulative()`, `coverageAll()` |
| `src/Common/Coverage.cpp` | `dumpCoverage()` — writes cumulative coverage as **file offsets** on client exit |
| `src/Processors/Formats/Impl/DWARFBlockInputFormat.cpp` | DWARF format reader — exposes ELF DWARF as SQL table |
| `tests/clickhouse-test` | Test runner; per-test INSERT (server + client), baseline INSERT, JIT suppression |
| `ci/workflows/nightly_coverage.py` | NightlyCoverage CI workflow |
| `ci/jobs/functional_tests.py` | Coverage-specific test runner setup |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Exports `coverage_log` to CIDB |
| `ci/jobs/scripts/find_tests.py` | Queries CIDB to find tests relevant to changed symbols |
| `ci/jobs/scripts/find_symbols.py` | Maps changed `(file, line)` pairs to symbol names via DWARF |

---

## Bugs Found and Fixed

### Bug 1: Client dump writes runtime VAs — server can't resolve them (FIXED)

**Root cause**: `dumpCoverage()` wrote raw runtime virtual addresses. The `clickhouse client` runs at a different ASLR base than the server → `SymbolIndex::findObject` returns null → empty symbols.

**Fix** (`src/Common/Coverage.cpp`): subtract binary load base before writing → stores file offsets.

### Bug 2: JIT settings enabled via random session settings (FIXED)

**Root cause**: `clickhouse-test` randomizes `compile_expressions`, `compile_sort_description`, etc. These cause the server to JIT-compile code into anonymous mmap. JIT-code return addresses → unresolvable → empty symbols in server-side row.

**Fix** (`tests/clickhouse-test`): force 6 JIT settings to 0 in `SettingsRandomizer.get_random_settings()` when `BuildFlags.SANITIZE_COVERAGE`.

### Bug 3: `BUILD_STRIPPED_BINARY=1` was useless in AMD_COVERAGE (FIXED)

Removed from `ci/jobs/build_clickhouse.py`. Created an unused stripped binary wasting build time.

### Minor: `coverageAll()` misparses PC table

`__sanitizer_cov_pcs_init` receives `(PC, PCFlags)` pairs but code treats as flat array. Not critical since `coverageAll()` is not exported.

---

## Symbol Normalization

### Why normalization matters

Demangled C++ symbols from DWARF and coverage have multiple forms:
- `DB::Foo::bar(arg1, arg2)` — with arg list
- `void DB::Foo::bar(arg1)` — with return type prefix
- `bool DB::getNewValueToCheck<DB::Settings>(...)` — template + return type
- `void DB::JoinStuff::JoinUsedFlags::setUsed<true, true>` — template function with no args (no `()`)
- `(anonymous namespace)::DistributedIndexAnalyzer::method(...)` — ClickHouse helper in anonymous namespace, NO `DB::` prefix

### Symbol categories in real CIDB

| Category | Count/day | Example |
|---|---|---|
| `starts_DB::` | 70.6M | `DB::MergeTreeData::loadDataParts(...)` |
| `std::prefix` | 12.3M | `std::vector<DB::Type>::method(...)` |
| `no_DB::_at_all` | 6.7M | LLVM anonymous namespace functions |
| `other` | 5.5M | `(anonymous namespace)::DistributedIndexAnalyzer::...` — ClickHouse helpers |
| `lowercase_rettype DB::` | 1.5M | `void DB::...`, `bool DB::...` |
| `STL_template<DB::>` | 0.5M | `AllocatorWithMemoryTracking<DB::Type>::allocate` |

**Important**: `(anonymous namespace)::DistributedIndexAnalyzer` is a ClickHouse class defined in `src/Interpreters/ClusterProxy/distributedIndexAnalysis.cpp` inside `namespace { using namespace DB; class DistributedIndexAnalyzer { ... }; }` — file-scope anonymous namespace, NOT inside `namespace DB`. Previous `DB::` filter missed all such classes.

### Why `WHERE position(sym, 'DB::') > 0` is wrong

For `std::vector<DB::Type>::method(arg)`: `position('DB::') = 12` is inside template args → extracts `DB::Type>::method(arg)` — garbage.

For `(anonymous namespace)::DistributedIndexAnalyzer(... DB::Connection* ...)`: `position('DB::') = 170` is inside function args → garbage.

### Export normalization (SQL in `export_coverage.py`)

1. Strip function argument list `(...)` using same-length placeholder for `(anonymous namespace)`
2. Strip trailing template args `<...>` only when the result ends with `>` (function template with no method name after)
3. **No namespace filter** — export ALL symbols. Return-type stripping is done in Python at query time.
4. Exclude `__client` rows: `WHERE NOT endsWith(test_name, '__client')`

### Query-time normalization (`normalize_symbol` in `find_tests.py`)

Uses bracket-depth algorithm to correctly handle ALL symbol forms:

```python
def normalize_symbol(symbol: str) -> str:
    """Extract qualified function name using bracket-depth tracking."""
    ANON = "(anonymous namespace)"
    modified = symbol.replace(ANON, "x" * len(ANON))

    # Find first '(' at <> depth 0 (arg list start)
    # Find last ' ' at <> depth 0 before that (return type / function name separator)
    depth = 0
    first_paren = len(modified)
    last_space = -1
    for i, c in enumerate(modified):
        if c == "<": depth += 1
        elif c == ">": depth -= 1
        elif depth == 0:
            if c == "(": first_paren = i; break
            elif c == " ": last_space = i

    func_name = symbol[last_space + 1 : first_paren]

    # Strip trailing function template args if symbol ends with '>'
    if func_name.endswith(">"):
        lt_pos = func_name.find("<")
        if lt_pos > 0:
            func_name = func_name[:lt_pos]

    return func_name if func_name else symbol
```

Examples (validated against 600 real CIDB symbols — 100% correct):
- `void DB::JoinStuff::JoinUsedFlags::setUsed<true, true>` → `DB::JoinStuff::JoinUsedFlags::setUsed`
- `std::shared_ptr<DB::Type> DB::Foo::getBar()` → `DB::Foo::getBar`
- `(anonymous namespace)::DistributedIndexAnalyzer::method(int)` → `(anonymous namespace)::DistributedIndexAnalyzer::method`
- `bool DB::getNewValueToCheck<DB::Settings>(DB::Settings const&)` → `DB::getNewValueToCheck`
- `DB::(anonymous namespace)::AggregateFunctionMinMax<T>::getName() const` → `DB::(anonymous namespace)::AggregateFunctionMinMax<T>::getName`
- `DB::HashedDictionaryImpl::HashedDictionaryParallelLoader<(DB::DictionaryKeyType)0, DB::HashedArrayDictionary<(DB::DictionaryKeyType)0, true>>::addBlock(DB::Block)` → `DB::HashedDictionaryImpl::HashedDictionaryParallelLoader<(DB::DictionaryKeyType)0, DB::HashedArrayDictionary<(DB::DictionaryKeyType)0, true>>::addBlock` — enum cast `(DB::DictionaryKeyType)0` inside `<>` is correctly preserved
- `DB::ContextAccess::checkAccessImplHelper<true, false, false, std::__1::basic_string_view<char>>(...)` → `DB::ContextAccess::checkAccessImplHelper`

**Known limitation**: `decltype(auto)` return type — `decltype(` is at bracket-depth 0 so it's treated as the arg list boundary, producing `decltype` as the result. Only affects STL variant dispatcher internals which are never queried from DWARF results for changed ClickHouse files.

---

## Detailed Test Selection Pipeline

### What a Symbol Is

A **demangled C++ function name** (after normalization — no arg list, no return type). NOT a file+line.

### Full Pipeline: Changed Line → Tests

**Step 1** — `find_symbols.py`: fetch PR diff from GitHub, parse `(file, line)` for changed C/C++ lines.

**Step 2** — `find_symbols.py`: DWARF query via `clickhouse local` (~85-180s for 6GB binary):
```sql
SELECT diff.filename, diff.line, binary.address, binary.linkage_name,
    if(empty(binary.linkage_name),
        demangle(addressToSymbol(binary.address)),
        demangle(binary.linkage_name)) AS symbol
FROM file('stdin', ...) AS diff
ASOF LEFT JOIN (
    SELECT decl_file, decl_line, linkage_name, ranges[1].1 AS address
    FROM file('{ch_path}', 'DWARF')
    WHERE tag = 'subprogram' AND (notEmpty(linkage_name) OR address != 0)
) AS binary
ON basename(diff.filename) = basename(binary.decl_file)
   AND diff.line >= binary.decl_line
```
The DWARF step is IO-bound (reads 6GB binary). Timing: 85s warm cache, 180s cold.

**Step 3** — `find_tests.py`: single batch `hasAllTokens` query to CIDB with `splitByNonAlpha` text index.

`normalize_symbol` produces the qualified function name (no return type, no args, no trailing template args). That string is passed directly to `hasAllTokens` — ClickHouse tokenizes it with the same `splitByNonAlpha` tokenizer used for the index, so no Python-side token splitting is needed.

```sql
SELECT norm_symbol, groupArray(DISTINCT test_name) AS tests
FROM checks_coverage_inverted
WHERE check_start_time > now() - interval 3 days
  AND check_name LIKE 'Stateless%'
  AND notEmpty(test_name)
  AND (  hasAllTokens(symbol, 'DB::MergeTreeData::loadDataParts')
      OR hasAllTokens(symbol, 'DB::PipelineExecutor::execute')
      OR ... )
GROUP BY norm_symbol
HAVING count(DISTINCT test_name) < least(
    greatest(toUInt64(total_tests * 5 / 100), 50),
    toUInt64(200)   -- MAX_TESTS_PER_SYMBOL
)
```

`hasAllTokens(symbol, 'DB::Foo::bar')` tokenizes the second arg to `['DB','Foo','bar']` and checks all are present in `symbol`. This matches:
- new-format (args stripped at export): `DB::Foo::bar` — exact
- old-format (full args still present): `DB::Foo::bar(Arg1, Arg2)` — arg tokens are extras, ignored
- all template instantiations: `DB::Foo<T>::bar(...)` — template arg tokens are extras, ignored

`norm_symbol` in the result is the CIDB symbol normalized in-SQL (arg list stripped), used to group instantiations.

### DWARF Format Schema

| Column | Type | Description |
|--------|------|-------------|
| `tag` | String | DWARF DIE tag |
| `linkage_name` | String | Mangled C++ name |
| `decl_file` | LowCardinality(String) | Source file |
| `decl_line` | UInt32 | Function start line |
| `ranges` | Array(Tuple(UInt64, UInt64)) | **(start, end)** address pairs |

---

## `checks_coverage_inverted` Table

### Recommended schema (local copy / CIDB)

```sql
CREATE TABLE default.checks_coverage_inverted
(
    symbol      LowCardinality(String),
    check_start_time DateTime('UTC'),
    check_name  LowCardinality(String),
    test_name   LowCardinality(String),

    -- splitByNonAlpha tokenizes 'DB::MergeTree::loadDataParts' into
    -- ['DB','MergeTree','loadDataParts'], enabling hasAllTokens() to match:
    --   - old-format symbols (with arg lists): tokens from function name still match
    --   - new-format symbols (args stripped): direct token match
    --   - all template instantiations: class/function template arg tokens are extras, ignored
    -- Direct read optimization: queries answered from posting lists without reading symbol column
    -- (~45-89x faster than column scan per ClickHouse docs).
    INDEX symbol_text_idx(symbol) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
-- Date first: check_start_time > now() - 3 days efficiently skips old granules.
-- Symbol second: within narrow date range, symbol lookups are fast.
-- test_name last: deduplicates identical (date, symbol, test) rows.
ORDER BY (toDate(check_start_time), symbol, test_name)
PARTITION BY toYYYYMM(check_start_time);

ALTER TABLE checks_coverage_inverted
    MATERIALIZE INDEX symbol_text_idx SETTINGS mutations_sync = 2;
```

**Previous CIDB schema** (production, no text index):
```sql
ORDER BY (symbol, check_start_time)  -- date is NOT the first key → scanning
                                      -- all symbols to find recent data hits
                                      -- max_rows_to_read limits easily
```
The production schema is inefficient for date-filtered queries — it must scan all symbol ranges to find recent rows. Local copy uses date-first ORDER BY which is dramatically faster.

**Filtering in queries**:
- `HAVING count(DISTINCT test_name) < least(greatest(total*5/100, 50), 200)` — exclude symbols in >5% of tests OR >200 tests (hot-path noise like `PipelineExecutor::execute`)
- `__client` rows excluded at export time (`WHERE NOT endsWith(test_name, '__client')`)
- Symbols normalized at export: arg list stripped, trailing template args stripped

---

## Problems with Current DWARF→Symbol Matching (Not Yet Fixed)

### Problem 1: Basename path matching — false positives

`basename(diff.filename) = basename(binary.decl_file)` — same-named files in different directories collide.

**Fix**: use last 2 path components: `arrayStringConcat(arraySlice(splitByChar('/', path), -2), '/')`

### Problem 2: No upper bound on function line range

ASOF JOIN only has lower bound (`decl_line ≤ changed_line`). Lines between functions assigned to preceding function.

**Fix**: use `ranges[1].2` + `addressToLine(ranges[1].2 - 1)` for end line. NOTE: `addressToLine` returns `String "file:line"`, NOT a tuple. Use `toUInt32(splitByChar(':', addressToLine(toUInt64(ranges[1].2 - 1)))[2])`. WARNING: this is slow (calls `addressToLine` for every DWARF subprogram row), reverting to ASOF JOIN is needed for performance.

### Problem 3: Template instantiations — hasAllTokens handles this

With `splitByNonAlpha` tokenizer and `hasAllTokens`, all template instantiations of the same function automatically match since template arg tokens are "extras" that don't need to be in the query tokens. No special handling needed.

### Problem 4: N+1 CIDB queries → FIXED

Replaced with single batch `hasAllTokens` query.

### Problem 5: Unresolved lines (file-level scope)

Lines in `#include` statements, namespace declarations, static constants, and function SIGNATURE lines (before the body `{`) cannot be resolved by ASOF JOIN. 4-5 per PR typically. Not fixable without line-level DWARF.

---

## `SymbolIndex::findSymbol` — How Address Resolution Works

```
findSymbol(addr):
    object = findObject(addr)            -- binary search over loaded objects
    if object:
        offset = addr - object.address_begin   -- runtime VA → file offset
    else:
        offset = addr                    -- fallback: treat directly as file offset
    return find(offset, symbols)         -- binary search over .symtab by file offset
```

The fallback path (treat as raw file offset) is why client dump file offsets resolve correctly — same mechanism as `system.stack_trace` after PR #82809.

---

## Summary of All Changes Made (PR #99513)

| File | Change | Why |
|------|--------|-----|
| `src/Common/Coverage.cpp` | `dumpCoverage()` subtracts load base → writes file offsets | Client's runtime VAs unresolvable by server with different ASLR base |
| `tests/clickhouse-test` | Force 6 JIT settings to 0 in `get_random_settings()` for SANITIZE_COVERAGE | JIT addresses in anonymous mmap → unresolvable → empty symbols in server row |
| `tests/clickhouse-test` | Server row named `{test}`, client row named `{test}__client` | Distinguish the two row types |
| `ci/jobs/build_clickhouse.py` | Remove `-DBUILD_STRIPPED_BINARY=1` from AMD_COVERAGE | Creates unused stripped binary; wastes build time |
| `ci/jobs/scripts/find_tests.py` | Single batch `hasAllTokens` query replaces N+1 per-symbol queries | Performance + handles old format, new format, and template instantiations |
| `ci/jobs/scripts/find_tests.py` | `normalize_symbol` uses bracket-depth algorithm | Correctly handles all symbol forms: void prefix, complex return types, anonymous namespace |
| `ci/jobs/scripts/find_tests.py` | `_symbol_to_tokens` for `hasAllTokens` | Splits normalized symbol into searchable tokens |
| `ci/jobs/scripts/find_tests.py` | Frequency filter: `least(5% of total, 100)` via HAVING | Excludes hot-path symbols AND client startup noise |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Remove namespace filter, export ALL symbols | `(anonymous namespace)::DistributedIndexAnalyzer` and other ClickHouse helpers now exported |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Exclude `__client` rows in export | Prevents inflating `count(distinct test_name)` which breaks frequency filter |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Strip only arg list + trailing template (no return type strip in SQL) | Return type stripping moved to Python `normalize_symbol` which handles all cases correctly |

---

## Use Cases for Coverage Data

1. **Targeted test selection** (current) — run only tests covering changed symbols
2. **Test ordering** — run tests covering most recently changed code first
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
