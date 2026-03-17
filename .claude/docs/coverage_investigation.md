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

After each test completes, TWO rows are inserted per test, **both using the plain test name**:

**Row 1 — Server-side INSERT** (`test_name = '{test_name}'`):
```sql
INSERT INTO system.coverage_log
WITH arrayDistinct(arrayFilter(x -> x != 0, coverageCurrent())) AS coverage_distinct
SELECT DISTINCT now(), '{test_name}', coverage_distinct,
       arrayMap(x -> demangle(addressToSymbol(x)), coverage_distinct)
```

**Row 2 — Client dump INSERT** (`test_name = '{test_name}'`):
The `clickhouse client` binary writes its `cumulative_coverage_array` to `coverage.{database}.{pid}` on exit. The test runner reads these files and inserts them under the same plain test name. Client rows are included in the export (not filtered out) so that test selection can find tests covering client-side code when client symbols change.

**Before tests start — baseline INSERT** (`test_name = ''`): captures server startup coverage.

Between tests: `SYSTEM RESET COVERAGE` zeroes `current_coverage_array` and resets guards to 1.

### Coverage Log Table Schema

```sql
CREATE TABLE IF NOT EXISTS system.coverage_log
(
    time DateTime,
    test_name String,    -- plain test name for both server and client rows
    coverage Array(UInt64),
    symbol Array(String)
) ENGINE = MergeTree ORDER BY test_name
```

### Export to CIDB

**File**: `ci/jobs/scripts/functional_tests/export_coverage.py`

Two sequential inserts, run once per nightly coverage job:

**Insert 1 → `checks_coverage_inverted`** (raw symbols, `hasAllTokens` queries):
- Exports ALL symbols, no namespace filter
- No SQL-side normalization — raw symbols stored as-is
- Text index (`splitByNonAlpha`) handles extra return-type and template-arg tokens at query time
- `WHERE notEmpty(sym)` only

**Insert 2 → `checks_coverage_inverted2`** (Python-normalized symbols, exact queries):
- Step 1: `clickhouse local` dumps `(sym, test_name)` pairs to a temp TSV file
- Step 2: `normalize_symbol` applied to each symbol in parallel via `ThreadPoolExecutor` (one chunk per CPU core); results deduplicated and written to a second TSV
- Step 3: `clickhouse local` inserts normalized TSV via `remoteSecure()`
- Stored symbol is the bare qualified name: `DB::Foo::bar` — no return type, no arg list, no template args

### Test Selection

**Files**: `ci/jobs/scripts/find_tests.py`, `ci/jobs/scripts/find_symbols.py`

```
PR diff (GitHub)
    ↓ parse changed (file, line) pairs for C/C++ files
    ↓ find_symbols.py: DWARF query → symbol names
    ↓ find_tests.py: CIDB query on checks_coverage_inverted (hasAllTokens)
    ↓               or checks_coverage_inverted2 (exact match)
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
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Exports `coverage_log` to CIDB (two inserts) |
| `ci/jobs/scripts/find_tests.py` | Queries CIDB to find tests relevant to changed symbols |
| `ci/jobs/scripts/find_symbols.py` | Maps changed `(file, line)` pairs to symbol names via DWARF |
| `ci/tests/test_normalize_symbol.py` | 61-test suite for `normalize_symbol` |

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

### Bug 4: SQL normalization in export lost ~27% of method name tokens (FIXED)

The SQL normalization in `export_coverage.py` had two bugs that caused `hasAllTokens` at query time to miss the method name for ~27% of stored symbols (validated against 2000 real CIDB symbols):

**Bug 4a — step 1: naive `find('(')` hit C-style casts inside template args**
C-style casts like `(char8_t)15` or `(DB::DictionaryKeyType)1` appear inside `<...>` template args. The SQL `position(modified, '(')` found these casts before the actual arg-list `(`. For `Foo<int, (char8_t)15>::method(args)`, the stored symbol was truncated to `Foo<int,` — method name `method` lost.

**Bug 4b — step 2: `position('<')` picked the first `<`, not the trailing one**
For class-template + function-template symbols like `DB::DecimalComparison<T,U>::vectorConstant<bool>`, `endsWith('>')` was true after step 1, so step 2 stripped from `position('<')` = first `<` (in the class template) onwards. Stored symbol became `void DB::DecimalComparison` — method name lost.

**Fix**: Remove SQL normalization entirely from insert 1. Raw symbols are stored; `hasAllTokens` is robust to extra return-type and template-arg tokens. `normalize_symbol` in Python handles correct normalization at query time.

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

### Export (insert 1): raw symbols, no normalization

Insert 1 stores raw symbols. `hasAllTokens` at query time is robust to extra tokens:
- Return-type prefix (`void`, `bool`, `std::shared_ptr<T>`) — extra tokens, ignored
- Arg list tokens — extra tokens, ignored
- Template arg tokens (`int`, `256ul`, type names) — extra tokens, ignored

### Query-time normalization (`normalize_symbol` in `find_tests.py`)

Produces the bare qualified function name with **all** template args stripped. The query `'DB::Foo::bar'` (tokens `['DB','Foo','bar']`) matches any stored symbol that contains those tokens — all template instantiations, all arg-list variants, any return-type prefix.

**Why strip class template args**: `hasAllTokens(stored, 'DB::Foo<int>::bar')` would require the token `int` to be present in the stored symbol. `hasAllTokens(stored, 'DB::Foo::bar')` matches all instantiations. Template arg tokens are extras in the stored symbol that the query need not mention.

Algorithm:
1. Replace `(anonymous namespace)` with same-length placeholder (so its `(` and `)` don't confuse depth tracking)
2. Neutralize `<` and `>` in operator tokens (`operator<<`, `operator>>`, `operator->`, etc.) with `_` using same-length substitution
3. Scan for first `(` at `<>` depth 0 → arg list start; last ` ` at depth 0 before that → return type boundary
4. Handle conversion operators: find last `operator ` at depth 0 before arg list; override last-space to the depth-0 space just before it
5. Strip ALL `<...>` blocks from the resulting `func_name` using the same two preprocessing passes (for correct handling of `operator<<` in the name and `(anonymous namespace)` in class names)

```python
# Simplified sketch — see find_tests.py for full implementation
def normalize_symbol(symbol: str) -> str:
    ANON = "(anonymous namespace)"
    modified = symbol.replace(ANON, "x" * len(ANON))
    # neutralize operator<< / >> / -> with same-length _ substitution
    modified = re.sub(r"(?<![A-Za-z_\d])operator([<>\-][<>=\-\*]*)",
                      lambda m: "operator" + m.group(1).replace("<","_").replace(">","_"),
                      modified)
    # find arg list '(' and return-type separator ' ' at <> depth 0
    ...
    func_name = symbol[last_space + 1 : first_paren]
    # strip ALL <...> template args from func_name
    ...
    return func_name or symbol
```

Examples (validated against 2000 real CIDB symbols — 0 errors):
- `void DB::JoinStuff::JoinUsedFlags::setUsed<true, true>()` → `DB::JoinStuff::JoinUsedFlags::setUsed`
- `std::shared_ptr<DB::Type> DB::Foo::getBar()` → `DB::Foo::getBar`
- `(anonymous namespace)::DistributedIndexAnalyzer::method(int)` → `(anonymous namespace)::DistributedIndexAnalyzer::method`
- `DB::(anonymous namespace)::AggregateFunctionMinMax<DB::SingleValueDataFixed<int>>::getName() const` → `DB::(anonymous namespace)::AggregateFunctionMinMax::getName`
- `DB::AggregateFunctionUniqCombined<char8_t, (char8_t)15, unsigned long>::add(char*, ...)` → `DB::AggregateFunctionUniqCombined::add`
- `wide::integer<128ul, unsigned int> DB::GCDLCMImpl<char8_t, wide::integer<128ul, unsigned int>, DB::(anonymous namespace)::GCDImpl<...>>::apply<wide::integer<128ul, unsigned int>>(...)` → `DB::GCDLCMImpl::apply`
- `std::__1::shared_ptr<DB::X>::~shared_ptr[abi:ne210105]()` → `std::__1::shared_ptr::~shared_ptr[abi:ne210105]`
- `AMQP::Field::operator AMQP::Array const&() const` → `AMQP::Field::operator AMQP::Array const&`
- `std::ostream& DB::Foo::operator<<(std::ostream&)` → `DB::Foo::operator<<`

**Known limitation**: `decltype(auto)` return type — `decltype(` is at bracket-depth 0 so it is treated as the arg-list boundary, producing `decltype` as the result. Only affects STL variant/tuple dispatch internals which are never queried from DWARF results for changed ClickHouse source files.

### Test suite

`ci/tests/test_normalize_symbol.py` — 61 tests covering:
- Basic forms: no return type, void/complex return type, const/volatile
- Anonymous namespaces: nested, with and without `DB::` prefix
- Function templates (trailing args stripped)
- Class templates (args stripped)
- Class + function templates combined
- Conversion operators (`operator int`, `operator AMQP::Array const&`, `operator unsigned int`)
- Non-conversion operators (`operator==`, `operator++`, `operator<<`, `operator>>`, `operator->`)
- `operator new` / `operator delete`
- Cast expressions inside template args: `(char8_t)15`, `(DB::DictionaryKeyType)1`, `_BitInt(8)`
- Deeply nested class templates with anonymous namespaces inside
- `std::function<void (char*&)>` in arg list
- ABI tags: `[abi:fe210105]`
- Clang `__1` inline namespace
- Destructors with `noexcept`
- `decltype(auto)` known limitation

---

## Detailed Test Selection Pipeline

### What a Symbol Is

A **demangled C++ function name** (after normalization — no arg list, no return type, no template args). NOT a file+line.

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

`normalize_symbol` produces the bare qualified name (no return type, no args, no template args). That string is passed directly to `hasAllTokens` — ClickHouse tokenizes it with the same `splitByNonAlpha` tokenizer used for the index, so no Python-side token splitting is needed.

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

Stores raw (un-normalized) symbols. Query with `hasAllTokens`.

### Schema

```sql
CREATE TABLE default.checks_coverage_inverted
(
    symbol      LowCardinality(String),
    check_start_time DateTime('UTC'),
    check_name  LowCardinality(String),
    test_name   LowCardinality(String),

    -- splitByNonAlpha tokenizes 'DB::MergeTree::loadDataParts' into
    -- ['DB','MergeTree','loadDataParts'], enabling hasAllTokens() to match:
    --   - raw symbols (with return type + arg list): function name tokens still present
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

---

## `checks_coverage_inverted2` Table

Stores Python-normalized symbols (bare qualified names). Query with exact `=` or `IN`.

### Schema

```sql
CREATE TABLE default.checks_coverage_inverted2
(
    symbol           LowCardinality(String),   -- bare qualified name: DB::Foo::bar
    check_start_time DateTime('UTC'),
    check_name       LowCardinality(String),
    test_name        LowCardinality(String),

    -- bloom_filter for exact equality: WHERE symbol = 'DB::Foo::bar'
    -- Eliminates non-matching granules with ~1% false-positive rate.
    -- No hasAllTokens needed — symbols are fully normalized at insert time.
    INDEX symbol_exact_idx(symbol) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (toDate(check_start_time), symbol, test_name)
PARTITION BY toYYYYMM(check_start_time);
```

### Query pattern

```sql
SELECT DISTINCT test_name
FROM checks_coverage_inverted2
WHERE check_start_time > now() - interval 3 days
  AND check_name LIKE 'Stateless%'
  AND symbol IN ('DB::Foo::bar', 'DB::Baz::qux', ...)
```

The `IN` list is the output of `normalize_symbol` applied to DWARF-derived symbols. No `hasAllTokens` needed — the stored symbols are already the bare names and equality is sufficient.

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
| `tests/clickhouse-test` | Client dump row uses plain test name (removed `__client` suffix) | Both rows stored under same name; client coverage included for test selection on client changes |
| `ci/jobs/build_clickhouse.py` | Remove `-DBUILD_STRIPPED_BINARY=1` from AMD_COVERAGE | Creates unused stripped binary; wastes build time |
| `ci/jobs/scripts/find_tests.py` | Single batch `hasAllTokens` query replaces N+1 per-symbol queries | Performance + handles old format, new format, and template instantiations |
| `ci/jobs/scripts/find_tests.py` | `normalize_symbol` uses bracket-depth algorithm, strips ALL template args | Correct handling of all symbol forms; bare query name matches all instantiations |
| `ci/jobs/scripts/find_tests.py` | Frequency filter: `least(5% of total, 200)` via HAVING | Excludes hot-path symbols from results |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Remove namespace filter, export ALL symbols | `(anonymous namespace)::DistributedIndexAnalyzer` and other ClickHouse helpers now exported |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Remove SQL normalization from insert 1; store raw symbols | SQL normalization had two bugs losing method name token for ~27% of symbols |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Remove `__client` filter; all rows exported | Client coverage used for test selection on client code changes |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Add insert 2 → `checks_coverage_inverted2` with Python-normalized symbols | Exact `=` queries; `bloom_filter` index; parallel normalization via `ThreadPoolExecutor` |
| `ci/tests/test_normalize_symbol.py` | New: 61-test suite for `normalize_symbol` | Regression coverage for all symbol forms and edge cases |

---

## Future Work: Better Test Selection

### The fundamental gap

Coverage data tells you **correlation** — "this code ran during this test."
What you actually want is **causation** — "this test would catch a bug in this code."

A test that runs `DB::Decimal::add` as infrastructure (it happens to be in the
query path) is very different from a test that *asserts* the correctness of decimal
arithmetic. Coverage treats them identically.

Two tractable improvements require no fundamental research, just engineering:

---

### Improvement 1: Token matching (easy, build today)

**Problem it solves**: Setting/configuration changes, SQL function renames, storage
engine parameters. When you change `merge_selector_algorithm`, DWARF gives you the
settings parser — which runs in every query, hits the frequency filter, gets dropped.
The test that does `SET merge_selector_algorithm = 'Simple'` is invisible to coverage
but trivially found by grep.

**How it works**:
1. Extract identifiers from the PR diff — CamelCase + snake_case split, ≥5 chars,
   filter language keywords
2. Grep `tests/queries/0_stateless/*.sql` (and reference files) for those tokens
3. Score by number of matching tokens, take top N

**Estimated gain**: ~55% recall improvement specifically for the ~20% of PRs that
touch settings/config/SQL function names. Small overhead (a grep, no new infra).

**What to store**: nothing new — this is a local grep at PR time.

---

### Improvement 2: Call graph traversal (moderate, requires one new artifact)

**Problem it solves**: When function B is changed, coverage only finds tests that
directly exercised B. But if A calls B, any test covering A would also catch a bug in B.
Coverage already captures this transitively *when the tests were run* — the gap is for
functions recently added or rarely called.

**How it works**:

```
Test → A → B (changed)
```

DWARF 5 records `DW_TAG_call_site` entries inside each `DW_TAG_subprogram` — every
call site in the binary. Query: "which functions call the changed function?" at depth 1-2.
Add those callers to the symbol set, then query the coverage table for their tests.

Depth 3+ is noise — everything eventually calls `String::find`.

**Required new artifact — `checks_call_graph` table**:

```sql
CREATE TABLE default.checks_call_graph
(
    callee      LowCardinality(String),  -- normalized: DB::Foo::bar
    caller      LowCardinality(String),  -- normalized: DB::Baz::qux
    binary_sha  LowCardinality(String),  -- which nightly build
    built_at    DateTime('UTC'),
    INDEX callee_idx(callee) TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplacingMergeTree(built_at)
ORDER BY (callee, caller);
```

Updated nightly: one row per `(caller, callee)` edge extracted from the coverage binary.

**Required new job — `export_call_graph.py`** (runs after the coverage binary is built,
before tests start):

```python
# clickhouse local reads DWARF call_site entries from the binary
query = """
    SELECT
        demangle(callee.linkage_name) AS callee_sym,
        demangle(caller.linkage_name) AS caller_sym
    FROM file('{binary}', 'DWARF') AS call_site
    JOIN file('{binary}', 'DWARF') AS callee
      ON call_site.call_target = callee.address
    JOIN file('{binary}', 'DWARF') AS caller
      ON call_site.parent = caller.offset
    WHERE call_site.tag = 'call_site'
      AND callee.tag = 'subprogram'
      AND caller.tag = 'subprogram'
      AND notEmpty(callee.linkage_name)
      AND notEmpty(caller.linkage_name)
"""
# INSERT into checks_call_graph via remoteSecure
```

NOTE: whether `DW_TAG_call_site` is exposed by ClickHouse's DWARF reader needs
verification. If not, alternative is binary disassembly: parse CALL instruction targets
from `llvm-objdump --disassemble` output.

**Change to `find_symbols.py`**: after DWARF lookup produces `changed_symbols`, query
`checks_call_graph` to expand to callers at depth 1-2:

```sql
-- depth-1 callers
SELECT caller FROM checks_call_graph
WHERE callee IN (changed_symbols)
  AND binary_sha = (SELECT max(binary_sha) FROM checks_call_graph)
```

Run this query twice (once more for depth-2), union with the original symbols, then pass
the expanded set to `find_tests.py` as before. No change to `find_tests.py`.

**No change to `find_tests.py`** — it already handles a list of symbols.

**Estimated gain**: ~5-15% recall on top of coverage, mostly for recently-added code
and low-frequency utility functions. Coverage already transitively captures most
call chains for stable code.

---

### Full pipeline with both improvements

```
PR diff
  │
  ├─ Token extraction (new) ─────────────────────────────────┐
  │    split CamelCase/snake_case, ≥5 chars                  │
  │    ↓                                                      │
  │  diff_tokens                                              │
  │    ↓                                                      │
  │  grep tests/queries/**/*.sql ─────────────────> token_tests (scored)
  │                                                           │
  ├─ DWARF lookup (existing) ────────────────────┐            │
  │    changed (file, line) → function names     │            │
  │    ↓                                         │            │
  │  changed_symbols                             │            │
  │    ↓                                         │            │
  │  call graph expansion (new)                  │            │
  │    checks_call_graph depth 1-2               │            │
  │    ↓                                         │            │
  │  expanded_symbols                            │            │
  │    ↓                                         ↓            │
  │  CIDB hasAllTokens query ───────────> coverage_tests      │
  │  (checks_coverage_inverted)                  │            │
  │                                              │            │
  └──────────────────────────────────────────────┴────────────┘
                                                  ↓
                                         union, rank by:
                                         1. direct coverage match (depth 0)
                                         2. caller coverage match (depth 1-2)
                                         3. token match score
                                         → top N tests
```

---

### How to measure the gain before building

Retrospective evaluation on existing CIDB data:

```python
# For each PR with test failures in the last 90 days:
# 1. Get diff tokens from GitHub
# 2. Grep test SQL for tokens
# 3. Compare overlap with actual failing tests
# Measure: recall = failing_tests_selected / all_failing_tests

for pr, failing_tests in get_failed_prs(last_90_days):
    tokens = extract_tokens(get_pr_diff(pr))
    token_selected = grep_tests(tokens)
    coverage_selected = get_coverage_selected(pr)

    print(f"PR {pr}:")
    print(f"  coverage recall:      {recall(coverage_selected, failing_tests):.0%}")
    print(f"  +token recall:        {recall(coverage_selected | token_selected, failing_tests):.0%}")
    print(f"  selected set size:    {len(coverage_selected)} → {len(coverage_selected | token_selected)}")
```

Run this before writing any production code. It costs one day and gives hard numbers
instead of estimates.

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
