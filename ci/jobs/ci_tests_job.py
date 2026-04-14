from ci.praktika.result import Result, ResultTranslator
from ci.praktika.utils import Utils, Shell

temp_path = f"{Utils.cwd()}/ci/tmp"

if __name__ == "__main__":
    pytest_report = f"{temp_path}/pytest_ci_tests.jsonl"
    pytest_log = f"{temp_path}/pytest_ci_tests.log"
    log = f"{temp_path}/ci_tests.log"

    Shell.run(
        "pip3 install 'pytest==8.3.5' --quiet",
        log_file=log,
        timeout=60,
    )

    Shell.run(
        f"python3 -m pytest ci/tests/ -k 'not test_e2e' --report-log={pytest_report} --log-file={pytest_log}",
        log_file=log,
        timeout=600,
    )

    test_result = ResultTranslator.from_pytest_jsonl(pytest_report_file=pytest_report)

    Result.create_from(
        name="CI Tests",
        results=test_result.results,
        status=test_result.status,
        info=test_result.info,
        files=[pytest_report, pytest_log, log],
    ).complete_job()
