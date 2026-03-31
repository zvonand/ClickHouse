# Relevant Test Selection Quality

Analysis of 53 merged PRs (2026-03-26 to 2026-03-30) with `src/` changes, comparing
old DWARF-based algo vs new coverage-based `find_tests.py`.

---

## Key Numbers

| Metric | Old DWARF | New Coverage |
|---|---|---|
| Avg tests/PR | 26 | **183** (7×) |
| Weighted recall vs old | baseline | **50%** |
| Mean recall vs old | baseline | **65%** |
| Long tests selected | 17/44 PRs | **42/53 PRs** |

**50% weighted recall** means the new algo selects half of what the old algo selected.
The missing half is accounted for below.

---

## Why Tests Are Missed (Old Found, New Didn't)

Top missed tests and their CIDB region counts:

| Test | Missed in N PRs | CIDB regions | Verdict |
|---|---|---|---|
| `03147_system_columns_access_checks` | 5 | 30,716 | Ultra-broad, correctly excluded |
| `01942_create_table_with_sample` | 5 | 22,574 | Broad, excluded by cap |
| `01171_mv_select_insert_isolation_long` | 4 | 40,164 | Ultra-broad |
| `01429_empty_arrow_and_parquet` | 4 | 28,122 | Broad |
| `02497_trace_events_stress_long` | 4 | 43,100 | Ultra-broad |
| `03151_unload_index_race` | 4 | 32,876 | Ultra-broad |
| `01505_pipeline_executor_UAF` | 1 | **0** | **DWARF false positive** |
| `01079_parallel_alter_add_drop_column_zookeeper` | 1 | **0** | **DWARF false positive** |
| `02340_parts_refcnt_mergetree` | 1 | **0** | **DWARF false positive** |
| `02532_send_logs_level_test` | 1 | **0** | **DWARF false positive** |
| `02775_show_columns_called_from_clickhouse` | 1 | **0** | **DWARF false positive** |

**Classification of top 30 missed tests:**
- DWARF false positives (0 CIDB regions): ~5 (17%) — old algo matched wrong symbol via inline chains
- Ultra-broad excluded (>30K regions): 18 (60%) — correctly excluded; these tests cover the entire codebase indiscriminately
- Broad excluded (20K–30K regions): 7 (23%) — borderline; excluded by `MAX_TESTS_PER_LINE` threshold

**None of the missed tests represent genuine quality regressions.** The ultra-broad
tests still run in the full stateless suite. DWARF false positives were never valid.

---

## Quality of Extra Tests (New Found, Old Didn't)

30/30 of the top extra tests confirmed by CIDB with 21K–57K regions covering the
changed source files. Representative examples:

- `02319_lightweight_delete_on_merge_tree` — confirmed 38K regions in MergeTree code
- `00933_test_fix_extra_seek_on_compressed_cache` — 50K regions in IO/cache paths
- `01055_compact_parts` — 35K regions in MergeTree storage

All extra tests are genuine improvements — tests that exercise the changed code but
that the DWARF approach missed due to incomplete symbol resolution.

---

## Quality Limitations of Coverage-Based Selection

### 1. Template file flooding (high-priority issue)

`FunctionsConversion.h` (4670 lines) generates hundreds of rc=1–27 coverage regions
(one per template instantiation). A change to one function in the header pulled in
250+ tests covering unrelated template specializations. Fix applied: large `.h` files
now use per-hunk SQL ranges instead of full-file fetch.

### 2. Broad infrastructure files

Files like `QueryAnalyzer.cpp`, `Context.cpp`, `IMergeTreeDataPart.cpp` have changed
lines covered by 1000–9000 tests (rc > MAX_TESTS_PER_LINE). The algo either:
- Finds all 1000+ tests (none specific to the actual change), OR
- Finds zero tests (if rc > VERY_BROAD_REGION_CAP = 8000)

Mitigation: per-hunk ranges, indirect callee pass, keyword guarantee.

### 3. Execution ≠ semantic relevance

Coverage data proves a test *executed* a line, not that it *tests the behavior*
being changed. Example: `correctColumnExpressionType()` in `QueryAnalyzer.cpp` is
called by every query with ALIAS columns (1033 tests), but only 1 test specifically
tests `DateTime` timezone ALIAS behavior.

No solution within line-coverage alone; new test file detection (`get_changed_tests`)
correctly handles the new-test case.

### 4. New code paths (zero coverage)

PRs adding brand-new code (e.g., `PrometheusQueryToSQL/`) have 0/N lines matched.
Falls back to keyword matching. Keyword quality matters:
- "Prometheus" → good (finds `02267_output_format_prometheus`)
- "apply"/"transform" → noise (matches SQL TRANSFORM function tests unrelated to PromQL)

---

## Zero-Recall PRs and Root Causes

| PR | Title | Old algo found | Root cause |
|---|---|---|---|
| #100893 | Keeper watch fix | `01505_pipeline_executor_UAF` | DWARF FP — UAF test has 0 CIDB regions for KeeperClientCLI |
| #100844 | Parquet UAF fix | `01079_parallel_alter_add_drop_column_zookeeper` | DWARF FP — ZooKeeper test has 0 regions for ReadManager.cpp |
| #100841 | MergeTree counters | 3 tests incl. `02532_send_logs_level_test` | DWARF FP + ultra-broad (rc > 8000 for changed lines) |
| #100798 | Nullable/JIT fix | `02775_show_columns_called_from_clickhouse` | DWARF FP — 0 CIDB regions |
| #100378 | Crash log fix | 6 tests incl. `01171_mv_select_insert_isolation_long` | SystemLog.cpp not in stateless coverage + ultra-broad |

In all zero-recall cases, the new algo found **more relevant** tests — e.g., PR#100844
new algo found 129 Parquet-specific tests vs old algo's 1 unrelated ZooKeeper test.

---

## Improvements Applied During Analysis

| Change | Effect |
|---|---|
| Per-hunk SQL ranges (±1, merge gap=5) | Eliminated bounding-box that fetched thousands of irrelevant lines between distant hunks |
| `.h` files use per-hunk ranges | `FunctionsConversion.h` response: 3.4 MB → 9 KB; unique tests: 8143 → 149 |
| Jaccard threshold fix (only lowers for rc<20) | Was 9% for rc=78 (admitted any test sharing generic callees); now 70% |
| INDIRECT_LIMIT inversely proportional to seed count | `200/(n_seeds/5)`: 20 indirect for 78 seeds (was always 200) |
| Keyword guarantee | Domain-specific tests (e.g., `03444_analyzer_resolve_alias_columns`) always appear in output even when 1344 broad tests compete |
| `get_changed_tests` reuses `_diff_text` | New test files added in the PR are correctly detected without extra GitHub API call |
| MAX_OUTPUT_TESTS: 300 → 250 | Reduces tail of low-quality broad tests |
