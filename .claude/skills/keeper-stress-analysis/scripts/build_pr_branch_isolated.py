#!/usr/bin/env python3
"""
Per-PR isolated effect, using PR-branch stress runs.

Method:
  1. Each PR branch's last commit's stress run = "PR HEAD" measurement.
  2. Pool ALL PR branch runs (across all PRs touching Keeper) by week.
  3. Compute the median of all-PR-branch runs in the SAME week as the PR HEAD's run.
     This median is the "PR-infra weekly baseline" — neutralises infra drift and
     factors out PR-specific changes (because median across many PRs cancels noise).
  4. PR isolated Δ = (PR HEAD value) − (weekly-pool median) for the matching scenario.

The result is a per-PR effect that's robust to:
  - PR-branch vs master infra differences (PR branches stay in their own pool)
  - Date-drift (weekly windowing)
  - Single-PR noise (median over many PR branches per week)

What it cannot fix:
  - Co-merged effects across the SAME branch: if the branch carries multiple
    independent PRs, Δ is joint.

PR set is read from pr_to_nightly.tsv (which carries the `branch`+`title` columns
populated by build_pr_nightly_map.py from pr_meta.tsv) — the script is generic
and works for any PR list, not just the original validation set.
"""
import csv
import datetime
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent


def load_pr_branch_runs():
    """Return all PR-branch stress runs from staging/pr_branches.tsv."""
    runs = []
    with open(ROOT / "staging" / "pr_branches.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            r["dt"] = datetime.datetime.fromisoformat(r["run_ended"]).replace(tzinfo=datetime.timezone.utc)
            r["rps_v"] = float(r.get("rps") or 0)
            r["p99_v"] = float(r.get("write_p99") or 0) if "write" in r["scenario"] else float(r.get("read_p99") or 0)
            runs.append(r)
    return runs


def load_pr_set():
    """Return list of (pr_number, title, branch) tuples from pr_to_nightly.tsv."""
    out = []
    path = ROOT / "pr_to_nightly.tsv"
    if not path.exists():
        return out
    with open(path) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            branch = r.get("branch", "").strip()
            if not branch:
                continue
            out.append((str(r["pr"]), r.get("title", ""), branch))
    return out


def iso_week(dt):
    yr, wk, _ = dt.isocalendar()
    return f"{yr}-W{wk:02d}"


def main():
    runs = load_pr_branch_runs()
    print(f"Loaded {len(runs)} PR-branch run-rows", file=sys.stderr)

    pr_set = load_pr_set()
    if not pr_set:
        print("No PRs found in pr_to_nightly.tsv (run build_pr_nightly_map.py first); exiting.", file=sys.stderr)
        return

    branches_of_interest = {br for _, _, br in pr_set}

    # Build weekly pool: scenario → week → list of (rps, p99) across ALL PR branches
    # in staging (not just our PR set — broader pool gives a tighter weekly baseline).
    pool = defaultdict(lambda: defaultdict(list))
    for r in runs:
        pool[r["scenario"]][iso_week(r["dt"])].append((r["rps_v"], r["p99_v"]))

    # Per-PR HEAD = latest run per (branch, scenario) for branches in our PR set.
    pr_head = {}
    for r in runs:
        if r["branch"] not in branches_of_interest:
            continue
        key = (r["branch"], r["scenario"])
        if key not in pr_head or r["dt"] > pr_head[key]["dt"]:
            pr_head[key] = r

    out_rows = []
    for pr, name, branch in pr_set:
        for scenario in ("prod-mix-no-fault[default]", "read-multi-no-fault[default]", "write-multi-no-fault[default]"):
            head = pr_head.get((branch, scenario))
            if not head:
                out_rows.append({
                    "pr": pr, "name": name, "branch": branch, "scenario": scenario,
                    "head_date": "—", "head_sha8": "",
                    "head_rps": "—", "pool_med_rps": "—", "iso_d_rps_pct": "—",
                    "head_p99": "—", "pool_med_p99": "—", "iso_d_p99_pct": "—", "n_pool": 0,
                })
                continue
            wk = iso_week(head["dt"])
            same_week_pool = [(rps, p99) for rps, p99 in pool[scenario].get(wk, []) if rps != head["rps_v"]]
            if len(same_week_pool) < 2:
                # Widen to ±1 ISO week. Use date arithmetic, not week-number ±1, so
                # we cross year/W01/W52-53 boundaries correctly.
                neighbours = []
                for delta_days in (-7, 7):
                    neighbour_wk = iso_week(head["dt"] + datetime.timedelta(days=delta_days))
                    neighbours += pool[scenario].get(neighbour_wk, [])
                same_week_pool += [(rps, p99) for rps, p99 in neighbours if rps != head["rps_v"]]
            if not same_week_pool:
                continue
            pool_rps = [rps for rps, _ in same_week_pool if rps > 0]
            pool_p99 = [p99 for _, p99 in same_week_pool if p99 > 0]
            if not pool_rps:
                continue
            pool_med_rps = statistics.median(pool_rps)
            pool_med_p99 = statistics.median(pool_p99) if pool_p99 else None
            d_rps = (head["rps_v"] - pool_med_rps) / pool_med_rps * 100 if pool_med_rps else 0
            d_p99 = ((head["p99_v"] - pool_med_p99) / pool_med_p99 * 100) if (pool_med_p99 and head["p99_v"]) else None
            out_rows.append({
                "pr": pr, "name": name, "branch": branch, "scenario": scenario,
                "head_date": head["run_ended"][:10], "head_sha8": head["sha8"],
                "head_rps": f"{head['rps_v']:.0f}",
                "pool_med_rps": f"{pool_med_rps:.0f}",
                "iso_d_rps_pct": f"{d_rps:+.1f}",
                "head_p99": f"{head['p99_v']:.1f}" if head["p99_v"] else "",
                "pool_med_p99": f"{pool_med_p99:.1f}" if pool_med_p99 else "",
                "iso_d_p99_pct": f"{d_p99:+.1f}" if d_p99 is not None else "",
                "n_pool": len(same_week_pool),
            })

    out_path = ROOT / "pr_branch_isolated.tsv"
    with open(out_path, "w", newline="") as f:
        cols = ["pr", "name", "branch", "scenario", "head_date", "head_sha8",
                "head_rps", "pool_med_rps", "iso_d_rps_pct",
                "head_p99", "pool_med_p99", "iso_d_p99_pct", "n_pool"]
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", lineterminator="\n")
        w.writeheader()
        for r in out_rows:
            w.writerow({c: r.get(c, "") for c in cols})
    print(f"Wrote {out_path} ({len(out_rows)} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
