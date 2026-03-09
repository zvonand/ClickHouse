"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Always succeeds — copilot errors are logged as warnings only.

GH_TOKEN is set to the robot token so copilot can access the Copilot API.
Before posting any gh CLI comments, copilot switches to the pre-authenticated
GitHub App account (clickhouse-gh[bot]) via `env -u GH_TOKEN gh auth switch`.
"""

import os
import shlex
import sys

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

_POST_AUTH = "env -u GH_TOKEN gh auth switch -u clickhouse-gh[bot]"


def _run(prompt):
    os.environ["GH_TOKEN"] = Secret.Config(
        name="/ci/robot-ch-test-poll-copilot", type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    try:
        Result.from_commands_run(
            name="copilot review",
            command=f"copilot -p {shlex.quote(prompt)} --allow-all-tools --model gpt-5.3-codex",
            with_info=True,
        )
    except Exception as e:
        print(f"WARNING: copilot review failed: {e}")
    finally:
        os.environ.pop("GH_TOKEN", None)


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    prompt = (
        f"Follow the instructions in .github/copilot-instructions.md. "
        f"Review the PR {info.pr_url}. "
        f"The repo is checked out at PR head. "
        f"Before posting any comments, switch to the app account by running: `{_POST_AUTH}`. "
        f"Post inline review comments on specific lines where applicable, and one summary comment. "
        f"Before posting, read existing review comments on this PR and do not duplicate ones already posted."
    )
    _run(prompt)


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    ci_report_url = info.get_report_url()

    prompt = (
        f"Fetch CI results for PR {info.pr_url} with: "
        f"node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links "
        f"(use --all to see all results, --test <name> to filter, --download-logs for deeper investigation). "
        f"If all checks passed — do nothing and stop. "
        f"If there are failures — the repo is checked out at PR head. "
        f"Look at the PR diff to understand what changed, then work backwards from the failures: "
        f"try to match each failure to the code changes and briefly explain why the change likely caused it. "
        f"Before posting, switch to the app account by running: `{_POST_AUTH}`. "
        f"Post a single comment on the PR with your findings."
    )
    _run(prompt)


if __name__ == "__main__":
    if "--pre" in sys.argv:
        pre()
    elif "--post" in sys.argv:
        post()
    else:
        print("Usage: copilot_review_job.py --pre | --post")
        sys.exit(1)

    Result.create_from(status=Result.Status.SUCCESS).complete_job()
