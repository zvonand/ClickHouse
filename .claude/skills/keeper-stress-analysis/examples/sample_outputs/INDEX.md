# Keeper Stress-Test Validation — file index

This directory holds the full validation pipeline + outputs for 33 Keeper-related
PRs against the master branch's nightly stress runs (current framework, in
production since `2026-03-25`).

## Reading guide

- **Start here**: [`REPORT.md`](REPORT.md) — the executive deliverable, with a
  per-PR table (§3), per-nightly evolution (§4), per-scenario evolution (§5),
  per-PR narrative cards (§6), thematic findings (§7), and **action items
  (§9a — read this if you only have 5 minutes)**.

- **Performance gains delivered**: [`PERFORMANCE_GAINS.md`](PERFORMANCE_GAINS.md) —
  the "what got better" view. Cumulative window-vs-window comparison (median
  of first 3 nightlies vs median of last 3) on every metric, every scenario,
  both backends. Top-10 ranked gains. Bottom-line summary.

- **Per-perf-PR data tables**: [`PR_PERF_TABLE.md`](PR_PERF_TABLE.md) —
  one section per perf PR (in-window + pre-threshold), every claim backed by
  a `pre → post (Δ%)` measurement on multiple metrics (rps, read_p99, write_p99,
  peak_mem, StorageLockWait, FileSync, TotalElapsed, etc.).

- **Per-PR progress attribution**: [`PR_PROGRESS.md`](PR_PROGRESS.md) — for each
  PR, what progress it intended to deliver, the metric signature that should
  show it, the measured Δ, and a verdict (visible / N/A / net-zero). The
  "what each PR actually moved" view.

- **Per-PR per-metric drill-down**: [`PER_PR_METRICS.md`](PER_PR_METRICS.md) —
  one section per PR with a "movers" table covering every scenario where any
  headline metric crossed the noise band.

- **Spreadsheet pivot source**: [`per_pr_metrics_long.tsv`](per_pr_metrics_long.tsv)
  — flat TSV, one row per (PR, scenario, backend, metric). Pivot in Excel /
  Google Sheets / pandas.

## Output artefacts

| File | Rows | Format | Description |
|---|---|---|---|
| `REPORT.md` | 1996+ lines | markdown | The full validation report |
| `PERFORMANCE_GAINS.md` | ~250 lines | markdown | Cumulative gains: baseline-window vs current-window medians, top-10 ranked improvements, pre-threshold PR contribution |
| `PR_PROGRESS.md` | 361 lines | markdown | Per-PR progress attribution: intent → expected metric → measured Δ → verdict |
| `cumulative_gains.tsv` | 1240 | TSV | Per (scenario, backend, metric) — baseline-window vs current-window medians + Δ |
| `cumulative_gains_summary.tsv` | 62 | TSV | Per (scenario, backend) — pivoted summary with rps_pct, p99_pct, mem_pct, etc. |
| `PER_PR_METRICS.md` | 533 lines | markdown | Per-PR movers tables (10 headline metrics × 62 scenarios) |
| `per_pr_metrics_long.tsv` | ~46k | TSV | Flat per-(PR, scenario, backend, metric) with `pre`, `post`, `delta_abs`, `delta_pct`, `baseline_kind` |
| `per_pr_summary.tsv` | 21 | TSV | One row per in-window PR with mechanical verdict + worst/best Δ |
| `per_pr_scenario_deltas.tsv` | 344 | TSV | One row per (PR, scenario, backend) with full Δ vector |
| `per_nightly_summary.tsv` | 37 | TSV | One row per master nightly with PRs landed since prior |
| `pr_to_nightly.tsv` | 21 | TSV | PR → first/last nightly mapping (primary + kind-matched fallbacks) |
| `pr_out_of_window.tsv` | 12 | TSV | PRs merged before threshold, listed for completeness |
| `pr_files_summary.tsv` | 21 | TSV | Files-touched fingerprint per PR (Keeper code? bench? docs?) |
| `merged_metrics.tsv` | 1093 | TSV | Joined wide table: 1093 (scenario, backend, commit) rows × 95 columns |

## Staging files (raw query output)

| File | Rows | Description |
|---|---|---|
| `staging/bench_summary.tsv` | 1094 | bench `summary` for all master nightlies since 2026-03-25 |
| `staging/prom_rates.tsv` | 21881 | prom-counter rates per (scenario, backend, commit, metric) — avg, p95, max |
| `staging/prom_gauges.tsv` | 21881 | prom gauges + cumulative-failure counters per (scenario, backend, commit, metric) |
| `staging/mntr.tsv` | 10941 | `mntr` 4LW metrics per (scenario, backend, commit, metric) |
| `staging/container.tsv` | 1095 | container CPU/memory rates and peaks |

## Pipeline scripts (re-runnable)

| File | Purpose | Inputs | Outputs |
|---|---|---|---|
| `queries/01_bench_summary.sql` | Bench summary staging query | `play.clickhouse.com` | `staging/bench_summary.tsv` |
| `queries/02_prom_rates.sql` | Prom counter rates query | ↑ | `staging/prom_rates.tsv` |
| `queries/03_prom_gauges.sql` | Prom gauges + failures query | ↑ | `staging/prom_gauges.tsv` |
| `queries/04_mntr.sql` | mntr 4LW query | ↑ | `staging/mntr.tsv` |
| `queries/05_container.sql` | Container CPU/mem query | ↑ | `staging/container.tsv` |
| `build_pr_nightly_map.py` | PR → nightly mapping | `../pr_meta.tsv`, `staging/bench_summary.tsv` | `pr_to_nightly.tsv`, `pr_out_of_window.tsv` |
| `build_metrics_table.py` | Join all staging into wide table | `staging/*.tsv` | `merged_metrics.tsv` |
| `compute_deltas.py` | Compute per-PR deltas + nightly summary | `merged_metrics.tsv`, `pr_to_nightly.tsv`, `../pr_meta.tsv` | `per_pr_scenario_deltas.tsv`, `per_pr_summary.tsv`, `per_nightly_summary.tsv` |
| `build_per_pr_metrics_tsv.py` | Flat per-PR per-metric TSV | `merged_metrics.tsv`, `pr_to_nightly.tsv` | `per_pr_metrics_long.tsv` |
| `build_per_pr_metrics.py` | Per-PR markdown movers tables | `merged_metrics.tsv`, `per_pr_summary.tsv` | `PER_PR_METRICS.md` |
| `build_report.py` | Full markdown report | All of the above | `REPORT.md` |
| `rebuild.sh` | One-shot rebuild from staging | `staging/*.tsv` | All `*.md` and `*.tsv` outputs |

## Reproduction

```bash
# Refresh staging from play.clickhouse.com (only needed if you want newer data):
for q in queries/0[1-5]_*.sql; do
  out="staging/$(basename "$q" .sql | sed 's/^[0-9][0-9]_//').tsv"
  curl -sG 'https://play.clickhouse.com/?user=play' --data-urlencode "query=$(cat "$q")" > "$out"
done

# Rebuild every output from staging:
./rebuild.sh
```

## Methodology notes

- **Threshold**: `2026-03-25` — when the current Keeper Stress Tests framework
  (with `*-no-fault[default|rocks]` and `*-fault[default|rocks]` scenarios)
  first ran on master.
- **Per-PR baselines**: each PR is mapped to (a) the last master nightly that
  finished before its merge, and (b) the first master nightly that started
  after its merge. Both are tracked separately for `no-fault` and `fault`
  sweeps (which alternate every ~12 h on master) so a PR landing between two
  fault sweeps still gets a usable no-fault baseline.
- **Co-merged PRs**: when several listed PRs merge in the same window between
  two nightlies, the same `pre→post` Δ is attributed to each of them. The
  `co_merged` column makes this explicit.
- **Significance bands**: `±5 %` for ratio metrics (rps, latency, memory),
  `±0.05 pp` for `error_rate`. Server-side hard-failure counters trigger a
  red flag at any non-zero value.

## Scope

- 33 PRs analysed; 21 in the post-threshold window, 12 before.
- 62 scenario × backend combinations (10 no-fault + 21 fault, each on `default`
  and `rocks` backends).
- 36 metrics tracked per (scenario, backend, commit): bench summary +
  prom counters/gauges + mntr + container.
- 37 master nightlies in the post-threshold window.

## Bottom line

**All 21 in-window PRs are clean.** Zero server-side failures across the
entire window. Read paths got faster (+11 % rps lift on `read-multi-no-fault`
between 2026-03-26 and 2026-04-30). Writes held flat. The visible
`error_rate` jump on `2026-04-04` is a bench-harness change (PR #100670
"keeper-bench: go faster"), not a Keeper regression.
