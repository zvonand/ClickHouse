"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Always succeeds — copilot errors are logged as warnings only.

Copilot writes the review to REVIEW_FILE; the script then posts it into the
existing CI bot comment using the "review" tag via GH.post_updateable_comment.
This way comment posting uses the job's pre-authenticated app credentials
(enable_gh_auth=True) and copilot only does analysis — no gh calls needed.
"""

import os
import shlex
import subprocess
import sys
import tempfile

from ci.praktika import Secret
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

REVIEW_FILE = "./ci/tmp/copilot_review.md"


def _run(prompt):
    token = Secret.Config(
        name="/ci/robot-ch-test-poll-copilot", type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    with tempfile.TemporaryDirectory() as gh_config_dir:
        try:
            subprocess.run(
                ["gh", "auth", "login", "--with-token"],
                input=token, text=True, check=True,
                env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
            )
            token = None
            Result.from_commands_run(
                name="copilot review",
                # --allow-all-tools: run non-interactively
                # --add-dir .: restrict file access to repo root (default,
                #   but explicit; do NOT add --allow-all-paths)
                command=f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
                        f"copilot -p {shlex.quote(prompt)} --allow-all-tools --add-dir . --model gpt-5.3-codex",
                with_info=True,
            )
        except Exception as e:
            print(f"WARNING: copilot review skipped: {e}")
            return

    try:
        with open(REVIEW_FILE) as f:
            review = f.read().strip()
    except FileNotFoundError:
        print(f"WARNING: review file not found: {REVIEW_FILE}")
        return

    if review:
        GH.post_updateable_comment(comment_tags_and_bodies={"review": review})


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    os.makedirs("./ci/tmp", exist_ok=True)
    prompt = (
        f"Follow the instructions in .github/copilot-instructions.md. "
        f"Review the PR {info.pr_url}. "
        f"The repo is checked out at PR head. "
        f"Post inline review comments directly on specific lines using gh CLI — "
        f"prefix every gh call with `env -u GH_CONFIG_DIR` so comments are posted "
        f"as the pre-authenticated app (not the Copilot robot account). "
        f"Before posting inline comments, read existing ones and do not duplicate them. "
        f"Write only the overall summary (no inline findings) as plain Markdown to {REVIEW_FILE}."
    )
    _run(prompt)


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    os.makedirs("./ci/tmp", exist_ok=True)
    ci_report_url = info.get_report_url()

    prompt = (
        f"Fetch CI results for PR {info.pr_url} with: "
        f"node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links "
        f"(use --all to see all results, --test <name> to filter, --download-logs for deeper investigation). "
        f"If all checks passed — write nothing and stop. "
        f"If there are failures — the repo is checked out at PR head. "
        f"Look at the PR diff to understand what changed, then work backwards from the failures: "
        f"try to match each failure to the code changes and briefly explain why the change likely caused it. "
        f"Write your findings as plain Markdown to {REVIEW_FILE}. "
        f"Do not post any GitHub comments — only write to the file."
        f"If you post any inline comments on specific lines, "
        f"prefix every gh call with `env -u GH_CONFIG_DIR` so comments are posted "
        f"as the pre-authenticated app (not the Copilot robot account)."
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
