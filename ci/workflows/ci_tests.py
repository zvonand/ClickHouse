from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

workflow = Workflow.Config(
    name="CI Tests",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="CI Tests",
            command="python3 ./ci/jobs/ci_tests_job.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            timeout=1200,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=True,
)

WORKFLOWS = [workflow]
