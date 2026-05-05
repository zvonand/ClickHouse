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
"""
import csv
import datetime
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent

# In-window PRs of interest, with their branches
PR_BRANCHES = {
    "99651":  "keeper-object-based-snapshots",
    "100876": "shared_mutex_keeper_log_store",
    "100778": "parallel-reads",
    "101502": "better-profiled-locks",
    "102586": "fix-keeper-spans-memory",
    "99491":  "snaps",
    "100834": "dk-keeper-client-watch",
    "100893": "fix/keeper-client-watch-stderr",
    "99484":  "piggy",
    "101524": "hanfei/fix-clang-tidy",
    "100773": "hot",
    "101427": "nuraft-streaming",
    "100998": "keeper_get_recursive_children",
    "101640": "dk-data-race-keeper-state-machine",
    "102599": "revert-100998-keeper_get_recursive_children",
    "100606": "keeper-jemalloc-webui",
    "102739": "fix-typos",
    "103064": "release-MemoryAllocatedWithoutCheck",
    "102629": "undur",
    "103025": "fix-create2-filllogelements",
    "103628": "fix-typos",
}

PR_NAMES = {
    "99651":  "Object-based snapshots",
    "100876": "shared_mutex KeeperLogStore",
    "100778": "Parallel reads",
    "101502": "Profiled lock overhead",
    "102586": "Fix OOMs huge multi",
    "99491":  "Snapshot code cleanup",
    "100834": "keeper-client watch",
    "100893": "watch stderr routing",
    "99484":  "Race fix read/session-close",
    "101524": "clang-tidy fix",
    "100773": "Hot-reload settings",
    "101427": "nuraft streaming opt-in",
    "100998": "getRecursiveChildren",
    "101640": "Data race fix",
    "102599": "Revert getRecursiveChildren",
    "100606": "jemalloc UI",
    "102739": "Fix typos",
    "103064": "MemoryAllocatedWithoutCheck",
    "102629": "last_durable_idx fix",
    "103025": "ZooKeeperCreate2Response fix",
    "103628": "Fix typos",
}


def load_pr_branch_runs():
    """Return all PR-branch stress runs from staging/pr_branches.tsv keyed by (branch, scenario, sha8)."""
    runs = []
    with open(ROOT / "staging" / "pr_branches.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            r["dt"] = datetime.datetime.fromisoformat(r["run_ended"]).replace(tzinfo=datetime.timezone.utc)
            r["rps_v"] = float(r.get("rps") or 0)
            r["p99_v"] = float(r.get("write_p99") or 0) if "write" in r["scenario"] else float(r.get("read_p99") or 0)
            runs.append(r)
    return runs


def iso_week(dt):
    yr, wk, _ = dt.isocalendar()
    return f"{yr}-W{wk:02d}"


def main():
    runs = load_pr_branch_runs()
    print(f"Loaded {len(runs)} PR-branch run-rows", file=sys.stderr)

    # Build weekly pool: scenario → week → list of (rps, p99)
    pool = defaultdict(lambda: defaultdict(list))
    for r in runs:
        pool[r["scenario"]][iso_week(r["dt"])].append((r["rps_v"], r["p99_v"]))

    # Per-PR HEAD = latest run per (branch, scenario)
    pr_head = {}
    for r in runs:
        if r["branch"] not in PR_BRANCHES.values():
            continue
        key = (r["branch"], r["scenario"])
        if key not in pr_head or r["dt"] > pr_head[key]["dt"]:
            pr_head[key] = r

    # For each PR, compute isolated Δ vs weekly pool median (excluding the PR's own observation)
    out_rows = []
    for pr, branch in PR_BRANCHES.items():
        for scenario in ("prod-mix-no-fault[default]", "read-multi-no-fault[default]", "write-multi-no-fault[default]"):
            head = pr_head.get((branch, scenario))
            if not head:
                out_rows.append({
                    "pr": pr, "name": PR_NAMES.get(pr, ""), "branch": branch, "scenario": scenario,
                    "head_date": "—", "head_rps": "—", "pool_med_rps": "—", "iso_d_rps_pct": "—",
                    "head_p99": "—", "pool_med_p99": "—", "iso_d_p99_pct": "—", "n_pool": 0,
                })
                continue
            wk = iso_week(head["dt"])
            same_week_pool = [(rps, p99) for rps, p99 in pool[scenario].get(wk, []) if rps != head["rps_v"]]
            if len(same_week_pool) < 2:
                # Widen to ±1 week
                neighbours = []
                yr, wn = wk.split("-W")
                for delta in (-1, 1):
                    new_wk = f"{yr}-W{int(wn)+delta:02d}"
                    neighbours += pool[scenario].get(new_wk, [])
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
                "pr": pr, "name": PR_NAMES.get(pr, ""), "branch": branch, "scenario": scenario,
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
