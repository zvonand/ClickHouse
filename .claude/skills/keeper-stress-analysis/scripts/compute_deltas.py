#!/usr/bin/env python3
"""
Compute per-PR and per-nightly deltas from merged_metrics.tsv + pr_to_nightly.tsv.

Outputs:
- per_pr_scenario_deltas.tsv : (pr, scenario, backend, metrics + Δ)
- per_pr_summary.tsv         : one row per PR with worst regression / best improvement
- per_nightly_summary.tsv    : one row per nightly run with PRs landed
"""
import csv
import datetime
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent

# Threshold below which PRs are considered out-of-window. Read from env var
# (set by rebuild.sh from its $2 arg); defaults to 2026-03-25 (when the
# current keeper-stress framework went live).
_threshold_str = os.environ.get("KEEPER_SKILL_THRESHOLD", "2026-03-25")
THRESHOLD = datetime.datetime.fromisoformat(_threshold_str).replace(tzinfo=datetime.timezone.utc)

PER_NIGHTLY_FIELDS = ["sha8", "earliest_ts", "scenarios_count", "kind", "prs_landed"]

# Headline metrics to track per scenario+backend
HEADLINE_METRICS = [
    ("rps",                    "higher_better"),
    ("read_p99_ms",            "lower_better"),
    ("write_p99_ms",           "lower_better"),
    ("error_pct",              "lower_better"),
    ("peak_mem_gb",            "lower_better"),
    ("p95_cpu_cores",          "lower_better"),
    ("zk_max_latency_max",     "lower_better"),
    ("FileSync_us_per_s_avg",  "lower_better"),
    ("StorageLockWait_us_per_s_avg", "lower_better"),
    ("OutstandingRequests_max", "lower_better"),
]

# Significance bands relative to baseline (deltas in percent unless absolute)
def classify(metric, pre, post):
    if pre is None or post is None:
        return "no-data"
    if metric == "error_pct":
        # Special: track error_pct in absolute pp terms
        if pre == 0 and post == 0:
            return "clean"
        if post < pre + 0.05:  # <0.05pp worsening = noise
            return "clean"
        if post < pre + 0.5:
            return "watch"
        return "regression"
    if pre == 0 and post == 0:
        return "clean"
    if pre == 0:
        return "watch"  # was zero, now non-zero
    pct = (post - pre) / abs(pre) * 100.0
    direction = HEADLINE_LOOKUP.get(metric, "lower_better")
    if direction == "higher_better":
        # higher_better: pct < 0 is bad
        if pct >= -5: return "clean"
        if pct >= -15: return "watch"
        return "regression"
    else:
        # lower_better: pct > 0 is bad
        if pct <= 5: return "clean"
        if pct <= 15: return "watch"
        return "regression"


HEADLINE_LOOKUP = dict(HEADLINE_METRICS)


def load_metrics():
    by_key = {}
    with open(ROOT / "merged_metrics.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            key = (r["scenario"], r["backend"], r["commit_sha"])
            by_key[key] = r
    return by_key


def load_metrics_indexed_by_sha8():
    """Map sha8 -> {(scenario,backend) -> metrics}."""
    out = defaultdict(dict)
    with open(ROOT / "merged_metrics.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            sha8 = r["commit_sha"][:8]
            out[sha8][(r["scenario"], r["backend"])] = r
    return out


def load_pr_map():
    rows = []
    with open(ROOT / "pr_to_nightly.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            rows.append(r)
    return rows


def parse_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def compute_pr_deltas():
    by_sha8 = load_metrics_indexed_by_sha8()
    prs = load_pr_map()
    scenarios_backends = set()
    for sb_map in by_sha8.values():
        for sb in sb_map:
            scenarios_backends.add(sb)

    deltas_rows = []  # one row per (PR, scenario, backend)
    pr_summary = []   # one row per PR

    # Pre-compute a list of nightlies with PRs that landed in each window
    # for the per-nightly summary
    for pr in prs:
        pre_sha = pr["pre_sha8"]
        post_sha = pr["post_sha8"]
        if not pre_sha or not post_sha:
            # No comparable nightly
            pr_summary.append({
                "pr": pr["pr"],
                "title": pr["title"],
                "merged_at": pr["merged_at"],
                "merge_sha8": pr["merge_sha8"],
                "pre_sha8": pre_sha,
                "post_sha8": post_sha,
                "co_merged": pr["co_merged"],
                "verdict": "not-yet-tested",
                "worst_regression": "",
                "best_improvement": "",
                "server_failures": 0,
            })
            continue

        pre_map  = by_sha8.get(pre_sha, {})
        post_map = by_sha8.get(post_sha, {})

        worst = None  # (metric, scenario, backend, pre, post, pct, verdict)
        best  = None
        scenario_verdicts = []
        server_failures = 0

        for sb in sorted(scenarios_backends):
            scenario, backend = sb
            pre_r  = pre_map.get(sb)
            post_r = post_map.get(sb)
            if pre_r is None or post_r is None:
                continue

            row = {
                "pr": pr["pr"],
                "title": pr["title"],
                "scenario": scenario,
                "backend": backend,
                "pre_sha8": pre_sha,
                "post_sha8": post_sha,
            }

            # Server-side failures
            sf_pre = (parse_float(pre_r.get("CommitsFailed_max",0)) or 0) + \
                     (parse_float(pre_r.get("SnapshotApplysFailed_max",0)) or 0) + \
                     (parse_float(pre_r.get("SnapshotCreationsFailed_max",0)) or 0) + \
                     (parse_float(pre_r.get("RejectedSoftMemLimit_max",0)) or 0)
            sf_post = (parse_float(post_r.get("CommitsFailed_max",0)) or 0) + \
                      (parse_float(post_r.get("SnapshotApplysFailed_max",0)) or 0) + \
                      (parse_float(post_r.get("SnapshotCreationsFailed_max",0)) or 0) + \
                      (parse_float(post_r.get("RejectedSoftMemLimit_max",0)) or 0)
            row["server_failures_post"] = int(sf_post)
            server_failures += int(sf_post)

            for metric, _ in HEADLINE_METRICS:
                pre_v = parse_float(pre_r.get(metric))
                post_v = parse_float(post_r.get(metric))
                row[f"{metric}_pre"]  = pre_v if pre_v is not None else ""
                row[f"{metric}_post"] = post_v if post_v is not None else ""
                if pre_v is not None and post_v is not None:
                    if pre_v == 0:
                        delta_pct = float("inf") if post_v > 0 else 0.0
                    else:
                        delta_pct = (post_v - pre_v) / abs(pre_v) * 100.0
                    row[f"{metric}_delta_pct"] = round(delta_pct, 1)
                    verdict = classify(metric, pre_v, post_v)
                    row[f"{metric}_verdict"] = verdict
                    scenario_verdicts.append((metric, scenario, backend, pre_v, post_v, delta_pct, verdict))

            deltas_rows.append(row)

        # Determine worst regression and best improvement across all scenarios
        regressions = [v for v in scenario_verdicts if v[6] in ("regression", "watch")]
        if regressions:
            # rank by absolute pct change
            regressions.sort(key=lambda x: abs(x[5]) if x[5] != float("inf") else 1e18, reverse=True)
            worst = regressions[0]

        improvements = [v for v in scenario_verdicts if v[6] == "clean"]
        # rank improvements by negative-delta on lower_better OR positive on higher_better
        ranked_improvements = []
        for v in scenario_verdicts:
            metric, sc, be, pre, post, pct, vd = v
            direction = HEADLINE_LOOKUP[metric]
            if direction == "lower_better" and pct < -10:
                ranked_improvements.append((v, -pct))
            elif direction == "higher_better" and pct > 5:
                ranked_improvements.append((v, pct))
        if ranked_improvements:
            ranked_improvements.sort(key=lambda x: x[1], reverse=True)
            best = ranked_improvements[0][0]

        # Assign overall verdict per PR
        if any(v[6] == "regression" for v in scenario_verdicts):
            verdict = "regression"
        elif any(v[6] == "watch" for v in scenario_verdicts):
            verdict = "watch"
        else:
            verdict = "clean"
        if server_failures > 0:
            verdict = "regression(server-failure)"

        def fmt_v(v):
            if v is None: return ""
            metric, sc, be, pre, post, pct, vd = v
            if pct == float("inf"):
                return f"{sc}[{be}] {metric}: 0 → {post:.2f}"
            return f"{sc}[{be}] {metric}: {pre:.2f} → {post:.2f} ({pct:+.1f}%)"

        pr_summary.append({
            "pr": pr["pr"],
            "title": pr["title"],
            "merged_at": pr["merged_at"],
            "merge_sha8": pr["merge_sha8"],
            "pre_sha8": pre_sha,
            "post_sha8": post_sha,
            "co_merged": pr["co_merged"],
            "verdict": verdict,
            "worst_regression": fmt_v(worst),
            "best_improvement": fmt_v(best),
            "server_failures": server_failures,
        })

    # Write deltas
    out_deltas = ROOT / "per_pr_scenario_deltas.tsv"
    if deltas_rows:
        all_keys = set()
        for r in deltas_rows:
            all_keys.update(r.keys())
        cols = ["pr", "title", "scenario", "backend", "pre_sha8", "post_sha8", "server_failures_post"] + \
               sorted([c for c in all_keys if c not in {"pr", "title", "scenario", "backend", "pre_sha8", "post_sha8", "server_failures_post"}])
        with open(out_deltas, "w") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
            w.writeheader()
            for r in deltas_rows:
                w.writerow(r)
    print(f"Wrote {out_deltas} ({len(deltas_rows)} rows)", file=sys.stderr)

    # Write PR summary
    out_summary = ROOT / "per_pr_summary.tsv"
    cols = ["pr","title","merged_at","merge_sha8","pre_sha8","post_sha8","co_merged","verdict","server_failures","worst_regression","best_improvement"]
    with open(out_summary, "w") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
        w.writeheader()
        for r in pr_summary:
            w.writerow(r)
    print(f"Wrote {out_summary} ({len(pr_summary)} rows)", file=sys.stderr)


def compute_nightly_summary():
    """For each master nightly, list PRs that landed since the prior nightly."""
    # Get distinct nightlies (sha8 with earliest run_ended) sorted chronologically
    by_sha8 = defaultdict(lambda: {"earliest": None, "scenarios": set()})
    with open(ROOT / "merged_metrics.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            sha8 = r["commit_sha"][:8]
            ts = datetime.datetime.fromisoformat(r["run_ended"]).replace(tzinfo=datetime.timezone.utc)
            cur = by_sha8[sha8]
            if cur["earliest"] is None or ts < cur["earliest"]:
                cur["earliest"] = ts
            cur["scenarios"].add(r["scenario"])

    nightlies = sorted(by_sha8.items(), key=lambda x: x[1]["earliest"])

    # Map PR merge time
    pr_meta = []
    with open(ROOT.parent / "pr_meta.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            r["merged_dt"] = datetime.datetime.fromisoformat(r["mergedAt"].replace("Z", "+00:00"))
            pr_meta.append(r)
    pr_meta.sort(key=lambda x: x["merged_dt"])

    rows = []
    prev_ts = THRESHOLD
    for sha8, info in nightlies:
        prs_landed = []
        for p in pr_meta:
            if prev_ts < p["merged_dt"] <= info["earliest"] and p["merged_dt"] >= THRESHOLD:
                prs_landed.append(p["pr"])
        rows.append({
            "sha8": sha8,
            "earliest_ts": info["earliest"].isoformat(),
            "scenarios_count": len(info["scenarios"]),
            # See note in build_pr_nightly_map.py — must match "-fault[" not "fault]"
            "kind": "fault" if any(("-fault[" in s and "-no-fault[" not in s) for s in info["scenarios"]) else "no-fault",
            "prs_landed": ",".join(prs_landed),
        })
        prev_ts = info["earliest"]

    out = ROOT / "per_nightly_summary.tsv"
    with open(out, "w") as f:
        w = csv.DictWriter(f, fieldnames=PER_NIGHTLY_FIELDS, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    if not rows:
        print(f"Wrote {out} (header-only — no master nightlies in window)", file=sys.stderr)
    else:
        print(f"Wrote {out} ({len(rows)} rows)", file=sys.stderr)


if __name__ == "__main__":
    compute_pr_deltas()
    compute_nightly_summary()
