"""
Copilot-based automated PR code review hook.

--pre: review code only (CI not yet complete)
--post: review code and CI results
"""

import shlex
import sys

from ci.praktika.info import Info
from ci.praktika.result import Result


def _run(prompt):
    return Result.from_commands_run(
        name="copilot review",
        command=f"copilot -p {shlex.quote(prompt)} --allow-all-tools",
        with_info=True,
    )


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return True

    prompt = (
        f"Review the PR {info.pr_url}. "
        f"The repo is checked out at PR head. "
        f"Post inline review comments on specific lines where applicable, and one summary comment. "
        f"Before posting, read existing review comments on this PR and do not duplicate ones already posted."
    )
    return _run(prompt).is_ok()


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return True

    ci_report_url = info.get_report_url()

    prompt = (
        f"Review the PR {info.pr_url}. "
        f"The repo is checked out at PR head. "
        f"Fetch CI results with: node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links "
        f"(use --all to see all results, --test <name> to filter by test name, "
        f"--download-logs to get logs.tar.gz for deeper investigation) "
        f"and include a CI summary in your review. "
        f"Post inline review comments on specific lines where applicable, and one summary comment. "
        f"Before posting, read existing review comments on this PR and do not duplicate ones already posted."
    )
    return _run(prompt).is_ok()


if __name__ == "__main__":
    if "--pre" in sys.argv:
        if not pre():
            sys.exit(1)
    elif "--post" in sys.argv:
        if not post():
            sys.exit(1)
    else:
        print("Usage: copilot_review.py --pre | --post")
        sys.exit(1)
