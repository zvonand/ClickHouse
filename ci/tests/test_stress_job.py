import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.stress.stress import escape_tsv_info
from ci.jobs.stress_job import read_test_results
from ci.praktika.result import Result


def test_escape_tsv_info_replaces_nul():
    assert escape_tsv_info("Invalid time zone: \0\n") == "Invalid time zone: \\0\\n"


def test_read_test_results_replaces_nul(tmp_path):
    results_path = tmp_path / "test_results.tsv"
    results_path.write_text(
        "Hung check failed, possible deadlock found\tFAIL\t\\N\tInvalid time zone: \0\n",
        encoding="utf-8",
    )

    results = read_test_results(results_path)

    assert len(results) == 1
    assert results[0].name == "Hung check failed, possible deadlock found"
    assert results[0].status == Result.Status.FAIL
    assert results[0].info == "Invalid time zone: \\0"
