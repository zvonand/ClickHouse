import argparse
import ast
import os
import re
import sys
from pathlib import Path

sys.path.append("./")

from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Query to fetch failed tests from CIDB for a given PR.
# Only returns tests from commit_sha/check_name combinations with fewer than 20 failures.
# This helps filter out commits with widespread test failures.
FAILED_TESTS_QUERY = """ \
 select distinct test_name
 from (
          select test_name, commit_sha, check_name
          from checks
          where 1
            and pull_request_number = {PR_NUMBER}
            and check_name LIKE '{JOB_TYPE}%'
            and check_status = 'failure'
            and match(test_name, '{TEST_NAME_PATTERN}')
            and test_status = 'FAIL'
            and check_start_time >= now() - interval 300 day
          order by check_start_time desc
              limit 10000
      )
 where (commit_sha, check_name) IN (
     select commit_sha, check_name
     from checks
     where 1
       and pull_request_number = {PR_NUMBER}
   and check_name LIKE '{JOB_TYPE}%'
   and check_status = 'failure'
   and test_status = 'FAIL'
   and check_start_time >= now() - interval 300 day
 group by commit_sha, check_name
 having count(test_name) < 20
     ) \
"""


class Targeting:
    INTEGRATION_JOB_TYPE = "Integration"
    STATELESS_JOB_TYPE = "Stateless"

    def __init__(self, info: Info):
        self.info = info
        if "stateless" in info.job_name.lower():
            self.job_type = self.STATELESS_JOB_TYPE
        elif "integration" in info.job_name.lower():
            self.job_type = self.INTEGRATION_JOB_TYPE
        else:
            self.job_type = None

    def get_changed_tests(self):
        # TODO: add support for integration tests
        result = set()
        if self.info.is_local_run:
            changed_files = Shell.get_output(
                "git diff --name-only $(git merge-base master HEAD)"
            ).splitlines()
        else:
            changed_files = self.info.get_changed_files()
        assert changed_files, "No changed files"

        for fpath in changed_files:
            if re.match(r"tests/queries/0_stateless/\d{5}", fpath):
                if not Path(fpath).exists():
                    print(f"File '{fpath}' was removed — skipping")
                    continue

                print(f"Detected changed test file: '{fpath}'")

                fname = os.path.basename(fpath)
                fname_without_ext = os.path.splitext(fname)[0]

                # Add '.' suffix to precisely match this test only
                result.add(f"{fname_without_ext}.")

            elif fpath.startswith("tests/queries/"):
                # Log any other changed file under tests/queries for future debugging
                print(
                    f"File '{fpath}' changed, but doesn't match expected test pattern"
                )

        return sorted(result)

    def get_previously_failed_tests(self):
        from ci.praktika.cidb import CIDB
        from ci.praktika.settings import Settings

        assert self.job_type, "Unsupported job type"
        assert (
            self.info.pr_number > 0
        ), "Find tests by previous failures applicable only for PRs"

        tests = []
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="nikf", passwd="s2djrs4SSfdoWp")
        if self.job_type == self.INTEGRATION_JOB_TYPE:
            test_name_pattern = "^test_"
        elif self.job_type == self.STATELESS_JOB_TYPE:
            test_name_pattern = "^[0-9]{5}_"
        else:
            assert False, f"Not supported job type [{self.job_type}]"
        query = FAILED_TESTS_QUERY.format(
            PR_NUMBER=self.info.pr_number,
            JOB_TYPE=self.job_type,
            TEST_NAME_PATTERN=test_name_pattern,
        )
        query_result = cidb.query(query, log_level="")
        # Parse test names from the query result
        for line in query_result.strip().split("\n"):
            if line.strip():
                # Split by whitespace and get the first column (test_name)
                parts = line.split()
                if parts:
                    test_name = parts[0]
                    tests.append(test_name)
        print(f"Parsed {len(tests)} test names: {tests}")
        tests = list(set(tests))
        return sorted(tests)

    @staticmethod
    def _escape_sql_string(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")

    # Absolute cap: a line covering more than this many tests is excluded
    # (too common code — carries no signal for targeted test selection).
    MAX_TESTS_PER_LINE = 200

    def get_tests_by_changed_lines(self, changed_lines: list) -> dict:
        """
        Query `checks_coverage_lines` for tests that cover each (filename, line_no) pair.

        `changed_lines` is a list of `(filename, line_no)` tuples.
        Returns a dict mapping each input tuple to a list of test names.
        """
        import time
        t0 = time.monotonic()

        if not changed_lines:
            return {fl: [] for fl in changed_lines}

        print(f"[find_tests] querying coverage for {len(changed_lines)} changed lines")

        conditions = " OR ".join(
            f"(endsWith(file, '{self._escape_sql_string(os.path.basename(f))}') AND line_start <= {ln} AND line_end >= {ln})"
            for f, ln in changed_lines
        )

        query = f"""
        SELECT
            file,
            line_start,
            line_end,
            groupArray(DISTINCT test_name) AS tests
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND ({conditions})
        GROUP BY file, line_start, line_end
        HAVING count(DISTINCT test_name) < {self.MAX_TESTS_PER_LINE}
        """

        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="nikf", passwd="s2djrs4SSfdoWp")
        t_query = time.monotonic()
        raw = cidb.query(query, log_level="")
        print(f"[find_tests] CIDB query: {time.monotonic()-t_query:.2f}s, response={len(raw)} bytes")

        # Parse TSV: file \t line_start \t line_end \t ['test1','test2',...]
        coverage_ranges: list = []
        for row in raw.strip().splitlines():
            if not row:
                continue
            parts = row.split("\t", 3)
            if len(parts) != 4:
                continue
            file_, line_start_s, line_end_s, tests_raw = parts
            try:
                line_start = int(line_start_s)
                line_end = int(line_end_s)
                tests = ast.literal_eval(tests_raw.strip())
                if isinstance(tests, list):
                    coverage_ranges.append((file_, line_start, line_end, tests))
            except (ValueError, SyntaxError):
                print(f"Failed to parse coverage row: {row[:100]}")

        # Map each input (filename, line_no) to its tests.
        result: dict = {}
        for filename, line_no in changed_lines:
            basename = os.path.basename(filename)
            matched: list = []
            for file_, line_start, line_end, tests in coverage_ranges:
                if file_.endswith(basename) and line_start <= line_no <= line_end:
                    matched.extend(tests)
            result[(filename, line_no)] = sorted(set(matched))

        total_unique_tests = len({t for tests in result.values() for t in tests})
        lines_with_tests = sum(1 for tests in result.values() if tests)
        print(
            f"[find_tests] done in {time.monotonic()-t0:.2f}s: "
            f"{lines_with_tests}/{len(changed_lines)} lines matched, "
            f"{total_unique_tests} unique tests selected"
        )
        return result

    def get_changed_or_new_tests_with_info(self):
        tests = self.get_changed_tests()
        info = f"Found {len(tests)} changed or new tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that were changed or added",
            status=Result.StatusExtended.OK,
            info=info,
        )

    def get_previously_failed_tests_with_info(self):
        tests = self.get_previously_failed_tests()
        # TODO: add job name to the result.info
        info = f"Found {len(tests)} previously failed tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that failed in previous runs",
            status=Result.StatusExtended.OK,
            info=info,
        )

    def get_changed_lines_from_diff(self):
        """
        Return a list of `(filename, line_no)` tuples from the PR diff.
        Uses `git diff` for local runs and `info.get_changed_files()` otherwise.
        """
        assert self.info.pr_number > 0, "Find tests by diff applicable for PRs only"
        diff_output = Shell.get_output(
            "git diff $(git merge-base master HEAD) --unified=0"
        )
        changed: list = []
        current_file = None
        for line in diff_output.splitlines():
            if line.startswith("+++ b/"):
                current_file = line[6:]
            elif line.startswith("@@ ") and current_file:
                # Parse @@ -old +new,count @@ and collect new lines
                import re as _re
                m = _re.search(r"\+(\d+)(?:,(\d+))?", line)
                if m:
                    start = int(m.group(1))
                    count = int(m.group(2)) if m.group(2) is not None else 1
                    for ln in range(start, start + count):
                        changed.append((current_file, ln))
        return changed

    def get_most_relevant_tests(self):
        """
        1. Gets changed lines from the PR diff.
        2. Queries `checks_coverage_lines` for tests covering those lines.
        3. Ranks tests by how many changed lines they cover (descending).
        4. Returns the ranked list and a `Result` with info about the findings.
        """
        changed_lines = self.get_changed_lines_from_diff()
        line_to_tests = self.get_tests_by_changed_lines(changed_lines)

        # Count how many changed lines each test covers.
        test_hit_count: dict = {}
        for tests in line_to_tests.values():
            for t in tests:
                test_hit_count[t] = test_hit_count.get(t, 0) + 1

        # Sort descending by hit count — tests covering more changed lines come first.
        ranked = sorted(test_hit_count, key=lambda t: -test_hit_count[t])

        info = "Tests found for lines:\n"
        if not line_to_tests:
            info += "  No changed lines found in diff\n"
        else:
            for (file_, line_), tests in line_to_tests.items():
                if tests:
                    info += f"  {file_}:{line_} -> {len(tests)} tests\n"
        info += f"Total unique tests: {len(ranked)}\n"
        if ranked:
            info += f"Top test: {ranked[0]} ({test_hit_count[ranked[0]]} lines covered)\n"

        return ranked, Result(
            name="tests found by coverage", status=Result.StatusExtended.OK, info=info
        )

    def get_all_relevant_tests_with_info(self):
        # Use a list to preserve insertion order and a seen set to deduplicate.
        # Priority: changed/new tests first, then previously failed, then
        # coverage-ranked tests (most changed lines covered first).
        seen: set = set()
        ranked: list = []
        results = []

        def add_tests(new_tests):
            for t in new_tests:
                if t not in seen:
                    seen.add(t)
                    ranked.append(t)

        # Integration tests run changed test suboptimally (entire module), it might be too long
        # limit it to stateless tests only
        if self.job_type == self.STATELESS_JOB_TYPE:
            changed_tests, result = self.get_changed_or_new_tests_with_info()
            add_tests(changed_tests)
            results.append(result)

        previously_failed_tests, result = self.get_previously_failed_tests_with_info()
        add_tests(previously_failed_tests)
        results.append(result)

        # TODO: Add coverage support for Integration tests
        if self.job_type == self.STATELESS_JOB_TYPE:
            try:
                covering_tests, result = self.get_most_relevant_tests()
                add_tests(covering_tests)
                results.append(result)
            except Exception as e:
                print(
                    f"WARNING: Failed to get coverage-based tests (best effort): {e}",
                    file=sys.stderr,
                )
                results.append(
                    Result(
                        name="tests found by coverage",
                        status=Result.StatusExtended.OK,
                        info=f"Skipped: {e}",
                    )
                )
                raise

        return ranked, Result(
            name="Fetch relevant tests",
            status=Result.Status.SUCCESS,
            info=f"Found {len(ranked)} relevant tests",
            results=results,
        )


if __name__ == "__main__":
    # local run tests
    parser = argparse.ArgumentParser(
        description="List tests covering changed lines for a PR by querying the coverage database."
    )
    parser.add_argument("pr", help="Pull request number")
    args = parser.parse_args()

    class InfoLocalTest:
        pr_number = int(args.pr)
        is_local_run = True
        job_name = "Stateless"

    info = InfoLocalTest()
    targeting = Targeting(info)
    changed_lines = targeting.get_changed_lines_from_diff()
    line_to_tests = targeting.get_tests_by_changed_lines(changed_lines)

    print("\nNo tests found for lines:")
    for (file, line), tests in line_to_tests.items():
        if tests:
            continue
        print(f"{file}:{line} -> NOT FOUND")

    all_tests: set = set()
    for tests in line_to_tests.values():
        all_tests.update(tests)

    print("\nTests found for lines:")
    for (file, line), tests in line_to_tests.items():
        if not tests:
            continue
        print(f"{file}:{line}:")
        for test in tests:
            print(f" - {test}")

    print(f"\nAll selected tests ({len(all_tests)}):")
    for test in sorted(all_tests):
        print(f" {test}")
