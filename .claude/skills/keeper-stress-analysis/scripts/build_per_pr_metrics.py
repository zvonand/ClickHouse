#!/usr/bin/env python3
"""For each in-window PR, emit a comprehensive per-scenario per-metric Δ table.

Output: PER_PR_METRICS.md
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent

# Headline metrics: (key_in_merged, label, unit, direction, fmt_pre, fmt_post, diff_kind)
# diff_kind: "pct" (percent change) or "abs" (absolute change in pp/units)
PR_MANUAL_VERDICT = {
    "100834": "clean (server unchanged)",
    "100893": "clean (client only)",
    "99484":  "clean",
    "99651":  "clean & broadly improving",
    "101524": "clean (clang-tidy only)",
    "101502": "clean & slightly improving",
    "99491":  "clean (cleanup only)",
    "100876": "clean & improving",
    "101427": "clean (opt-in feature)",
    "100773": "clean (no-op in stress)",
    "100778": "clean & improving",
    "100998": "net-zero (reverted)",
    "101640": "clean",
    "102599": "net-zero (revert)",
    "102586": "clean & protective",
    "100606": "clean (UI only)",
    "102739": "clean (text only)",
    "103064": "clean (instrumentation)",
    "102629": "clean",
    "103025": "clean (logging only)",
    "103628": "not yet tested",
}

METRICS = [
    ("rps",                          "rps",           "",     "↑", 0, 0, "pct"),
    ("read_p99_ms",                  "read p99",      " ms",  "↓", 1, 1, "pct"),
    ("write_p99_ms",                 "write p99",     " ms",  "↓", 1, 1, "pct"),
    ("error_pct",                    "err",           "%",    "↓", 2, 2, "pp"),
    ("peak_mem_gb",                  "peak mem",      " GB",  "↓", 2, 2, "pct"),
    ("p95_cpu_cores",                "p95 cpu",       "",     "↓", 2, 2, "pct"),
    ("zk_max_latency_max",           "zk_max_lat",    " ms",  "↓", 0, 0, "pct"),
    ("FileSync_us_per_s_avg",        "FileSync",      " µs/s", "↓", 0, 0, "pct"),
    ("StorageLockWait_us_per_s_avg", "LockWait",      " µs/s", "↓", 0, 0, "pct"),
    ("OutstandingRequests_max",      "Outstanding",   "",     "↓", 0, 0, "pct"),
]

NO_FAULT = [
    "prod-mix-no-fault", "read-no-fault", "write-no-fault",
    "read-multi-no-fault", "write-multi-no-fault",
    "churn-no-fault", "list-heavy-no-fault",
    "large-payload-no-fault", "single-hot-get-no-fault", "multi-large-no-fault",
]
FAULT = [
    "prod-mix-fault", "read-fault", "write-fault",
    "read-multi-fault", "write-multi-fault",
    "churn-fault", "list-heavy-fault",
    "large-payload-fault", "single-hot-get-fault", "multi-large-fault",
    "latency-multi-region-fault", "latency-wan-fault", "latency-jitter-fault",
    "latency-slow-follower-fault", "latency-single-follower-fault",
    "latency-wan-client-fault", "prod-mix-cpu-leader-fault",
    "write-multi-cpu-all-fault", "large-payload-mem-fault",
    "prod-mix-mem-fault", "write-disk-latency-fault",
]


def is_fault_scenario(sc: str) -> bool:
    return "-fault[" in sc and "-no-fault[" not in sc


def parse_tsv(path):
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def to_float(s, default=None):
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def fmt_num(v, places):
    if v is None:
        return "—"
    av = abs(v)
    if av >= 1e6:
        return f"{v/1e6:.{places}f}M"
    if av >= 1e3:
        return f"{v/1e3:.{places}f}k"
    return f"{v:.{places}f}"


def fmt_delta(pre, post, kind, places):
    if pre is None or post is None:
        return "—"
    if kind == "pp":
        return f"{post - pre:+.{places}f}pp"
    # pct
    if pre == 0:
        if post == 0:
            return "0%"
        return "from 0"
    return f"{(post-pre)/abs(pre)*100:+.1f}%"


def cell(pre, post, places, kind):
    pre_s = fmt_num(pre, places)
    post_s = fmt_num(post, places)
    delta = fmt_delta(pre, post, kind, max(places, 2) if kind == "pp" else 0)
    return f"{pre_s} → {post_s} ({delta})"


def severity(pre, post, kind, direction):
    """Return (sort_priority, is_significant) where higher priority = bigger move."""
    if pre is None or post is None:
        return (0.0, False)
    if kind == "pp":
        absdiff = abs(post - pre)
        # 1pp threshold for error_pct
        sig = absdiff >= 0.05
        return (absdiff, sig)
    if pre == 0 and post == 0:
        return (0.0, False)
    if pre == 0:
        return (1e6, post != 0)
    pct = (post - pre) / abs(pre) * 100
    return (abs(pct), abs(pct) >= 5)


def main():
    metrics_rows = parse_tsv(ROOT / "merged_metrics.tsv")
    by_sb_sha = defaultdict(dict)
    for r in metrics_rows:
        by_sb_sha[(r["scenario"], r["backend"])][r["commit_sha"][:8]] = r

    pr_summary = parse_tsv(ROOT / "per_pr_summary.tsv")

    out = []
    P = out.append
    P("# Per-PR detailed metrics — every in-window PR × every scenario × every headline metric")
    P("")
    P("_Companion to [`REPORT.md`](REPORT.md). For each in-window PR, this file lists `pre → post (Δ)` for each headline metric on each of the 62 scenario/backend combinations. For the executive summary and per-PR narrative cards, see `REPORT.md`. For machine-readable raw data, see [`per_pr_metrics_long.tsv`](per_pr_metrics_long.tsv)._")
    P("")
    P("Columns: each headline metric is rendered as `pre → post (Δ%)` or `pre → post (Δpp)` for `err%`. Direction arrow in the header: ↑ = higher is better, ↓ = lower is better.")
    P("")
    P("To keep tables readable, each PR's matrix is split into:")
    P("- **Top movers** — scenarios where _any_ headline metric crossed the noise band (≥ 5 % for ratio metrics, ≥ 0.05 pp for `err%`).")
    P("- **Quiet scenarios** — collapsed into a single line at the end.")
    P("")
    P("**Co-merged PRs share their pre→post window**, so the matrix for #99484, #99651, and #101524 is identical (they all merged in the `fdf46ee1 → e02b59d7` window). The verdict in `REPORT.md §6` and §3's `Files` column is what disambiguates which PR likely caused which signal.")
    P("")
    P("---")
    P("")

    # Header for the per-PR matrix
    metric_headers = [f"{lab}{u} {dirn}" for k, lab, u, dirn, *_ in METRICS]

    for pr in pr_summary:
        prn = pr["pr"]
        title = pr["title"]
        pre_sha = pr["pre_sha8"]
        post_sha = pr["post_sha8"]
        merged = pr["merged_at"].split("T")[0]
        co = pr["co_merged"]

        P(f"## #{prn} — _{title}_")
        P("")
        P(f"- **Merged**: {merged}  ·  **Pre→Post nightly**: `{pre_sha or '—'}` → `{post_sha or '—'}`  ·  **Co-merged**: {co or '—'}")
        P(f"- **Server-failure counters (post)**: `{pr['server_failures']}`")
        P(f"- **Curated verdict** (see `REPORT.md §6`): {PR_MANUAL_VERDICT.get(prn, pr['verdict'])}")
        P("")

        if not pre_sha or not post_sha:
            P("> No post-merge nightly available — PR landed too late for this analysis or no comparable run.")
            P("")
            continue

        scenarios = NO_FAULT + FAULT
        backends = ["default", "rocks"]

        # Build per-(scenario, backend) row: list of (metric_label, pre, post, places, kind, direction)
        all_rows = []
        for short in scenarios:
            for be in backends:
                sc = f"{short}[{be}]"
                pre_r = by_sb_sha.get((sc, be), {}).get(pre_sha)
                post_r = by_sb_sha.get((sc, be), {}).get(post_sha)
                if pre_r is None and post_r is None:
                    continue
                cells = []
                max_priority = 0.0
                any_sig = False
                for key, label, unit, direction, p_pre, p_post, kind in METRICS:
                    pre_v = to_float((pre_r or {}).get(key))
                    post_v = to_float((post_r or {}).get(key))
                    cells.append(cell(pre_v, post_v, p_pre, kind))
                    pri, sig = severity(pre_v, post_v, kind, direction)
                    if pri > max_priority:
                        max_priority = pri
                    if sig:
                        any_sig = True
                all_rows.append((sc, be, cells, max_priority, any_sig))

        # Sort by max_priority descending
        all_rows.sort(key=lambda x: x[3], reverse=True)
        movers = [r for r in all_rows if r[4]]
        quiet  = [r for r in all_rows if not r[4]]

        # Render movers (cap at 15 to keep manageable)
        if movers:
            P("**Movers** (scenarios where ≥ 1 metric crossed the noise band):")
            P("")
            P("| Scenario | " + " | ".join(metric_headers) + " |")
            P("|" + "|".join(["---"] * (1 + len(METRICS))) + "|")
            for r in movers[:15]:
                sc, be, cells, _, _ = r
                P(f"| `{sc}` | " + " | ".join(cells) + " |")
            if len(movers) > 15:
                P(f"")
                P(f"_…{len(movers) - 15} more mover scenarios omitted; see `per_pr_scenario_deltas.tsv` for the full list._")
            P("")
        else:
            P("**No scenarios crossed the noise band on any headline metric** — change is invisible in the bench/server metrics for this PR.")
            P("")

        if quiet:
            P(f"**Quiet** (no metric crossed the noise band): `{len(quiet)}` scenario/backend combinations. Sample: " +
              ", ".join(f"`{r[0]}`" for r in quiet[:6]) + ("…" if len(quiet) > 6 else "") + ".")
            P("")
        P("---")
        P("")

    out_path = ROOT / "PER_PR_METRICS.md"
    with open(out_path, "w") as f:
        f.write("\n".join(out))
    print(f"Wrote {out_path} ({len(out)} lines)", file=sys.stderr)


if __name__ == "__main__":
    main()
