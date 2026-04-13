from ci.praktika.result import Result
from ci.praktika.utils import Utils

temp_path = f"{Utils.cwd()}/ci/tmp"

if __name__ == "__main__":
    Result.from_pytest_run(
        command="ci/tests/",
        name="CI Tests",
        pytest_report_file=f"{temp_path}/pytest_ci_tests.jsonl",
        pytest_logfile=f"{temp_path}/pytest_ci_tests.log",
        logfile=f"{temp_path}/ci_tests.log",
        timeout=600,
    ).complete_job()
