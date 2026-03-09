"""
Copilot-based automated PR code review hook.

--pre: review code only (CI not yet complete)
--post: review code and CI results
"""

import os
import shlex
import sys

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result


def _run(prompt):
    os.environ["GH_TOKEN"] = Secret.Config(
        name="/ci/robot-ch-test-poll-copilot", type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    result = Result.from_commands_run(
        name="copilot review",
        command=f"copilot -p {shlex.quote(prompt)} --allow-all-tools --model gpt-5.3-codex",
        with_info=True,
    )
    os.environ.pop("GH_TOKEN", None)
    return result


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return True

    prompt = (
        f"Follow the instructions in .github/copilot-instructions.md. "
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
        f"Fetch CI results for PR {info.pr_url} with: "
        f"node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links "
        f"(use --all to see all results, --test <name> to filter, --download-logs for deeper investigation). "
        f"If all checks passed — do nothing and stop. "
        f"If there are failures — the repo is checked out at PR head. "
        f"Look at the PR diff to understand what changed, then work backwards from the failures: "
        f"try to match each failure to the code changes and briefly explain why the change likely caused it. "
        f"Post a single comment on the PR with your findings."
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
