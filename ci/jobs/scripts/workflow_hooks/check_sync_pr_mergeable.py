import sys
import traceback

from praktika.info import Info
from praktika.utils import Shell

SYNC_REPO = "ClickHouse/clickhouse-private"


def check():
    info = Info()

    if not info.pr_number > 0:
        print(f"WARNING: Invalid or unknown pr number: {info.pr_number}")
        return True

    sync_pr_numbers = Shell.get_output(
        f"gh pr list --state open --head sync-upstream/pr/{info.pr_number} --repo {SYNC_REPO} --json number --jq '.[].number'",
        verbose=True,
    ).splitlines()
    sync_pr_numbers = [n for n in sync_pr_numbers if n.strip()]

    if len(sync_pr_numbers) == 0:
        print("WARNING: No open Sync PR found - skipping check")
        return True

    if len(sync_pr_numbers) > 1:
        print(
            f"ERROR: Expected at most one open Sync PR for branch sync-upstream/pr/{info.pr_number}, "
            f"found {len(sync_pr_numbers)}: {sync_pr_numbers}"
        )
        return False

    sync_pr_number = sync_pr_numbers[0]

    mergeable = Shell.get_output(
        f"gh pr view {sync_pr_number} --repo {SYNC_REPO} --json mergeable --jq .mergeable",
        verbose=True,
    ).strip()

    if mergeable == "CONFLICTING":
        print(
            f"ERROR: Sync PR #{sync_pr_number} in {SYNC_REPO} has conflicts and cannot be merged"
        )
        return False

    print(
        f"Sync PR #{sync_pr_number} in {SYNC_REPO} is mergeable (state: {mergeable})"
    )
    return True


if __name__ == "__main__":
    try:
        if not check():
            sys.exit(1)
    except Exception:
        traceback.print_exc()
