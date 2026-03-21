"""
Fetch old-algorithm test list from S3 CI targeted check job logs.
URL: https://s3.amazonaws.com/clickhouse-test-reports/PRs/{PR}/{SHA}/stateless_tests_arm_asan_targeted/job.log
"""
import json, re, os, glob, urllib.request, urllib.error, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://s3.amazonaws.com/clickhouse-test-reports/PRs"
JOB  = "stateless_tests_arm_asan_targeted"
TEST_RE = re.compile(r"^\d{5}_\S+\.(?:sql|sh|expect)\.?$")

def get_sha(pr):
    r = subprocess.run(
        ["gh", "pr", "view", str(pr), "--repo", "ClickHouse/ClickHouse",
         "--json", "commits", "--jq", ".commits[-1].oid"],
        capture_output=True, text=True, timeout=15)
    sha = r.stdout.strip()
    return sha if len(sha) == 40 else None

def fetch_tests(pr, sha):
    url = f"{BASE}/{pr}/{sha}/{JOB}/job.log"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "curl/7.0"})
        with urllib.request.urlopen(req, timeout=25) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return None, url, f"HTTP {e.code}"
    except Exception as e:
        return None, url, str(e)

    # Parse: lines after "--order=random --" up to "| ts "
    collecting = past_dd = False
    tokens = []
    for line in text.splitlines():
        clean = re.sub(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] ", "", line)
        if not collecting:
            if "clickhouse-test" in clean and "--order=random" in clean:
                collecting = True
        if collecting:
            if "| ts " in clean:
                tokens.extend(clean.split("| ts")[0].split())
                break
            tokens.extend(clean.split())

    # Extract everything after "--"
    tests, after = [], False
    for t in tokens:
        if t == "--":
            after = True; continue
        if after:
            t = t.rstrip(".")
            if not t.endswith((".sql", ".sh", ".expect")):
                # try to recover truncated names like "04049_arrow_parquet_writer_uuid"
                if re.match(r"\d{5}_\S+", t):
                    t += ".sql"
            if TEST_RE.match(t + ("" if t.endswith((".sql",".sh",".expect")) else ".sql")):
                tests.append(t if t.endswith((".sql",".sh",".expect")) else t)
    return tests, url, None

def process(pr_file):
    d = json.load(open(pr_file))
    pr = d["pr"]
    old = d.get("old_algo", {})
    if old.get("fetched_targeted"):
        return pr, old.get("tests", []), "cached"

    sha = get_sha(pr)
    if not sha:
        d["old_algo"] = {**old, "fetched_targeted": True, "found": False, "sha": None, "reason": "no_sha"}
        json.dump(d, open(pr_file, "w"), indent=2)
        return pr, [], "no_sha"

    tests, url, err = fetch_tests(pr, sha)
    if tests is None:
        d["old_algo"] = {**old, "fetched_targeted": True, "found": False, "sha": sha, "url": url, "reason": err}
    else:
        d["old_algo"] = {**old, "fetched_targeted": True, "found": bool(tests), "tests": tests, "sha": sha, "url": url}
    json.dump(d, open(pr_file, "w"), indent=2)
    return pr, tests or [], err

if __name__ == "__main__":
    os.chdir("/home/nik/work/ClickHouse2")
    files = sorted(glob.glob("tmp/pr_analysis/pr_*.json"))
    print(f"Fetching S3 targeted job logs for {len(files)} PRs (16 workers)…")
    found = done = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(process, f): f for f in files}
        for fut in as_completed(futs):
            pr, tests, status = fut.result()
            done += 1
            n = len(tests)
            if n > 0:
                found += 1
                print(f"  #{pr}: {n} old tests  [{status or 'ok'}]")
            if done % 50 == 0:
                print(f"  ── {done}/{len(files)} done, {found} with data ──")
    print(f"\nDone: {done} PRs, {found} with old algo tests")
