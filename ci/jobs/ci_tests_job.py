from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.result import Result
from ci.praktika.utils import Utils

temp_path = f"{Utils.cwd()}/ci/tmp"

if __name__ == "__main__":
    CH = ClickHouseProc()

    res = ClickHouseProc.download_binary()
    res = res and CH.install_light()
    res = res and CH.start_light()

    if res:
        test_result = Result.from_pytest_run(
            name="CI Tests",
            command="ci/tests/",
            pytest_report_file=f"{temp_path}/pytest_ci_tests.jsonl",
            pytest_logfile=f"{temp_path}/pytest_ci_tests.log",
            logfile=f"{temp_path}/ci_tests.log",
            timeout=600,
        )
    else:
        test_result = Result.create_from(
            name="CI Tests",
            status=Result.Status.ERROR,
            info="Failed to start ClickHouse server",
        )

    CH.terminate(force=True)
    test_result.complete_job()
