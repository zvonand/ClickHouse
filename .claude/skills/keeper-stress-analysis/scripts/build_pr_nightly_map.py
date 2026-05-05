#!/usr/bin/env python3
"""Build PR -> first-post-merge nightly mapping with co-merged PR list."""
import csv
import datetime
import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent
THRESHOLD = datetime.datetime(2026, 3, 25, 0, 0, 0, tzinfo=datetime.timezone.utc)


def parse_pr_meta() -> list[dict]:
    rows = []
    with open(ROOT.parent / "pr_meta.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            r["merged_dt"] = datetime.datetime.fromisoformat(r["mergedAt"].replace("Z", "+00:00"))
            r["pr"] = int(r["pr"])
            rows.append(r)
    rows.sort(key=lambda x: x["merged_dt"])
    return rows


def parse_nightlies() -> list[dict]:
    """Return list of distinct master commits with run timestamps from bench_summary."""
    by_sha: dict[str, dict] = {}
    with open(ROOT / "staging" / "bench_summary.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            sha = r["commit_sha"]
            ended = datetime.datetime.fromisoformat(r["run_ended"]).replace(tzinfo=datetime.timezone.utc)
            # Scenario name contains "-fault[" (e.g. "prod-mix-fault[default]") for fault sweeps
            # and "-no-fault[" for no-fault sweeps. We need to distinguish carefully because
            # both backend tags (`[default]`/`[rocks]`) end with "]" and "default" contains "fault".
            kind = "fault" if ("-fault[" in r["scenario"] and "-no-fault[" not in r["scenario"]) else "no-fault"
            if sha not in by_sha:
                by_sha[sha] = {"commit_sha": sha, "sha8": r["sha8"], "earliest": ended, "latest": ended, "kinds": set([kind])}
            else:
                by_sha[sha]["earliest"] = min(by_sha[sha]["earliest"], ended)
                by_sha[sha]["latest"]   = max(by_sha[sha]["latest"], ended)
                by_sha[sha]["kinds"].add(kind)
    nightlies = list(by_sha.values())
    nightlies.sort(key=lambda x: x["earliest"])
    return nightlies


def has_nofault(n: dict) -> bool:
    return "no-fault" in n["kinds"]


def has_fault(n: dict) -> bool:
    return "fault" in n["kinds"]


def main():
    prs = parse_pr_meta()
    nightlies = parse_nightlies()

    # Build PR -> first-post-merge nightly
    in_window = [p for p in prs if p["merged_dt"] >= THRESHOLD]
    out_window = [p for p in prs if p["merged_dt"] < THRESHOLD]

    print(f"PRs in window (>= {THRESHOLD.date()}): {len(in_window)}", file=sys.stderr)
    print(f"PRs out of window: {len(out_window)}", file=sys.stderr)
    print(f"Distinct master nightlies in staging: {len(nightlies)}", file=sys.stderr)

    rows_out = []
    # For each in-window PR: find first nightly whose earliest run-time is >= merged_dt
    # AND find last nightly whose latest run-time is <= merged_dt (the pre-baseline).
    # Additionally compute fallback baselines that match the *kind* of run that the PR
    # missed: if a PR's `post` is fault-only, store the next no-fault nightly as
    # `post_nofault_sha8`; analogously for fault-only.
    for pr in in_window:
        # Use pr merged_dt as the cut-off
        post = next((n for n in nightlies if n["earliest"] >= pr["merged_dt"]), None)
        pre  = None
        for n in nightlies:
            if n["latest"] <= pr["merged_dt"]:
                pre = n
            else:
                break
        # First post-merge no-fault and fault nightlies (independent of which kind `post` is)
        post_nofault = next(
            (n for n in nightlies if n["earliest"] >= pr["merged_dt"] and has_nofault(n)),
            None)
        post_fault = next(
            (n for n in nightlies if n["earliest"] >= pr["merged_dt"] and has_fault(n)),
            None)
        pre_nofault = None
        pre_fault = None
        for n in nightlies:
            if n["latest"] <= pr["merged_dt"]:
                if has_nofault(n):
                    pre_nofault = n
                if has_fault(n):
                    pre_fault = n
            else:
                break
        # Co-merged PRs: any other in-window PR whose mergedAt falls in (pre.latest, post.earliest]
        co_merged = []
        if post is not None:
            lo = pre["latest"] if pre is not None else THRESHOLD
            for q in in_window:
                if q["pr"] == pr["pr"]:
                    continue
                if lo < q["merged_dt"] <= post["earliest"]:
                    co_merged.append(q["pr"])
        rows_out.append({
            "pr": pr["pr"],
            "title": pr["title"],
            "merged_at": pr["mergedAt"],
            "merge_sha8": pr["mergeCommit"][:8],
            "pre_sha8":  pre["sha8"] if pre else "",
            "pre_run":   pre["latest"].isoformat() if pre else "",
            "post_sha8": post["sha8"] if post else "",
            "post_run":  post["earliest"].isoformat() if post else "",
            "pre_nofault_sha8":  pre_nofault["sha8"] if pre_nofault else "",
            "post_nofault_sha8": post_nofault["sha8"] if post_nofault else "",
            "pre_fault_sha8":    pre_fault["sha8"] if pre_fault else "",
            "post_fault_sha8":   post_fault["sha8"] if post_fault else "",
            "co_merged": ",".join(str(x) for x in sorted(co_merged)),
        })

    out_path = ROOT / "pr_to_nightly.tsv"
    with open(out_path, "w") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(rows_out)
    print(f"Wrote {out_path} ({len(rows_out)} rows)", file=sys.stderr)

    # Out-of-window list
    oow_path = ROOT / "pr_out_of_window.tsv"
    with open(oow_path, "w") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["pr", "title", "merged_at", "merge_sha8"])
        for p in out_window:
            w.writerow([p["pr"], p["title"], p["mergedAt"], p["mergeCommit"][:8]])
    print(f"Wrote {oow_path} ({len(out_window)} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
