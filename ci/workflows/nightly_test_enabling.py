import copy

from praktika import Workflow, Job

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames, BuildTypes, JobNames, RunnerLabels
from ci.defs.job_configs import JobConfigs, common_build_job_config

w_schedule = Workflow.Config(
    name="NightlyCoverage",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *common_build_job_config.set_post_hooks(
            post_hooks=[
                "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
                "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
            ],
        ).parametrize(
            Job.ParamSet(
                parameter=BuildTypes.AMD_DARWIN,
                provides=[ArtifactNames.CH_AMD_DARWIN_BIN],
                runs_on=RunnerLabels.AMD_LARGE,
            ),
        ),
        *JobConfigs.darwin_fast_test_jobs,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 2 * * *"],
)
w_dispatch = copy.copy(w_schedule)
w_dispatch.event = Workflow.Event.PULL_REQUEST # Workflow.Event.DISPATCH REMOVEME
w_dispatch.branches = []
w_dispatch.base_branches = [BASE_BRANCH]

WORKFLOWS = [
    w_schedule,
    w_dispatch
]
