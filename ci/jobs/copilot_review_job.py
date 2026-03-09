"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Always succeeds — copilot errors are logged as warnings only.

Auth split:
- The robot Copilot account token is stored in an isolated GH_CONFIG_DIR
  temp directory via `gh auth login`. Copilot reads credentials from there
  for model inference. GH_TOKEN is never set in the environment.
- Comment posting uses the pre-authenticated clickhouse-gh[bot] app (the
  default gh config). Since gh(1) prefers GH_CONFIG_DIR over the default
  config when the env var is set, every gh CLI call that posts must be
  prefixed with `env -u GH_CONFIG_DIR` to fall back to the app credentials.
"""

import os
import shlex
import subprocess
import sys
import tempfile

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

# Prefix all gh CLI calls that post comments with this so gh falls back to
# the default config dir (clickhouse-gh[bot]) instead of the robot token.
_GH = "env -u GH_CONFIG_DIR gh"


def _run(prompt):
    token = Secret.Config(
        name="/ci/robot-ch-test-poll-copilot", type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    with tempfile.TemporaryDirectory() as gh_config_dir:
        subprocess.run(
            ["gh", "auth", "login", "--with-token"],
            input=token, text=True, check=True,
            env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
        )
        token = None
        try:
            Result.from_commands_run(
                name="copilot review",
                command=f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
                        f"copilot -p {shlex.quote(prompt)} --allow-all-tools --model gpt-5.3-codex",
                with_info=True,
            )
        except Exception as e:
            print(f"WARNING: copilot review failed: {e}")


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    prompt = (
        f"Follow the instructions in .github/copilot-instructions.md. "
        f"Review the PR {info.pr_url}. "
        f"The repo is checked out at PR head. "
        f"IMPORTANT: for every gh CLI call (reading PR data, posting comments, creating reviews) "
        f"prefix it with `env -u GH_CONFIG_DIR` (e.g. `{_GH} pr view ...`, `{_GH} api ...`). "
        f"This ensures comments are posted as the pre-authenticated app, not the Copilot robot account. "
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
        f"IMPORTANT: for every gh CLI call (reading PR data, posting comments) "
        f"prefix it with `env -u GH_CONFIG_DIR` (e.g. `{_GH} pr view ...`, `{_GH} api ...`). "
        f"This ensures comments are posted as the pre-authenticated app, not the Copilot robot account. "
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
