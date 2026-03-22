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
        changed_files = Shell.get_output(
            f"gh pr diff {self.info.pr_number} --repo ClickHouse/ClickHouse --name-only"
        ).splitlines() if self.info.is_local_run else self.info.get_changed_files()
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
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
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

    @staticmethod
    def _stored_path(path: str) -> str:
        """Convert a repo-relative diff path to the stored coverage path format.

        Coverage data is built with -ffile-prefix-map=/ClickHouse=. so all
        source paths are stored as ./src/... in checks_coverage_lines.
        Strip any leading ./ from the diff path then re-add the ./ prefix.
        """
        p = path.replace("\\", "/").lstrip("./")
        return "./" + p

    # Absolute cap: a line covering more than this many tests is excluded
    # (too common code — carries no signal for targeted test selection).
    MAX_TESTS_PER_LINE = 200

    # Regions wider than this are considered "broad" (low signal).
    NARROW_REGION_MAX_LINES = 20

    # Synthetic width for tests found via indirect-call (virtual dispatch) co-occurrence.
    # Lower than SIBLING_DIR_WIDTH (10000) because callee co-occurrence is a stronger
    # signal than directory proximity: tests that call the same vtable slots as primary
    # tests are exercising the same interface, not merely a related file.
    INDIRECT_CALL_WIDTH = 3000

    # Synthetic width assigned to tests found via same-directory sibling file
    # expansion (secondary pass).  Must be >> NARROW_REGION_MAX_LINES so they
    # never get the narrow-tier bonus and always rank below direct hits.
    SIBLING_DIR_WIDTH = 10000

    # Per-pass score multipliers applied on top of the 1/(width×rc) signal.
    # Pass 1 (direct line coverage) is the baseline.  Passes 2 and 3 use
    # secondary signals and are discounted so that even a weak direct hit
    # outranks the strongest indirect or sibling hit.
    PASS_WEIGHT_DIRECT   = 1.0   # Pass 1: test directly covers changed lines
    PASS_WEIGHT_INDIRECT = 0.3   # Pass 3: test shares virtual-dispatch callees with primary tests
    PASS_WEIGHT_SIBLING  = 0.1   # Pass 2: test covers a sibling file in the same source directory
    PASS_WEIGHT_KEYWORD  = 0.05  # Fallback: test filename contains domain keywords from changed files

    def get_tests_by_changed_lines(self, changed_lines: list) -> dict:
        """
        Query `checks_coverage_lines` for tests that cover each (filename, line_no) pair.

        `changed_lines` is a list of `(filename, line_no)` tuples.
        Returns a dict mapping each input tuple to a list of `(test_name, region_width)`
        tuples, where `region_width = line_end - line_start + 1`.  The region width is
        used by the caller to weight test scores (narrow regions = high signal).

        Regions with `region_test_count > MAX_TESTS_PER_LINE` are excluded: such lines
        are infrastructure code (e.g. `Settings.cpp` entry points) that all tests touch
        and therefore carry no signal for targeted test selection.
        """
        import time
        t0 = time.monotonic()

        if not changed_lines:
            return {fl: [] for fl in changed_lines}

        # Filter to files that are actually tracked in checks_coverage_lines.
        # Coverage is only collected for compiled C++ sources (src/, programs/,
        # utils/ etc.).  Querying test scripts, docs, CI configs, or contrib files
        # always returns zero rows and wastes CIDB quota — skip them up front.
        COVERAGE_TRACKED_PREFIXES = ("src/", "programs/", "utils/", "base/")
        coverage_lines = [
            (f, ln)
            for f, ln in changed_lines
            if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
        ]
        skipped = len(changed_lines) - len(coverage_lines)
        if skipped:
            print(
                f"[find_tests] skipping {skipped} lines in non-tracked files "
                f"(test scripts, docs, CI, contrib)"
            )

        # Return the original (full) key-set in the result dict so that
        # callers always get one entry per input line (with empty list for
        # non-tracked files).
        non_tracked_keys = {(f, ln) for f, ln in changed_lines if (f, ln) not in set(coverage_lines)}
        base_result: dict = {k: [] for k in non_tracked_keys}

        if not coverage_lines:
            print("[find_tests] no coverage-tracked files changed — skipping CIDB query")
            base_result.update({(f, ln): [] for f, ln in changed_lines})
            return base_result

        # Group changed lines by file so we query with `file IN (…)` instead of
        # one condition per line.  A PR touching 13 files but 1000+ lines would
        # otherwise generate a URL too long for the CIDB HTTP endpoint.
        files_to_lines: dict = {}
        for f, ln in coverage_lines:
            files_to_lines.setdefault(self._stored_path(f), set()).add(ln)

        unique_files = sorted(files_to_lines)
        print(
            f"[find_tests] querying coverage for {len(coverage_lines)} changed lines "
            f"across {len(unique_files)} files"
        )

        # Build one condition per file: file='X' AND line_end >= min_changed AND line_start <= max_changed.
        # This pre-filters to regions that *could* overlap any changed line in that file,
        # avoiding a full table scan per file while keeping the query size O(files) not O(lines).
        per_file_conds = " OR ".join(
            f"(file = '{self._escape_sql_string(f)}'"
            f" AND line_end >= {min(lines)} AND line_start <= {max(lines)})"
            for f, lines in sorted(files_to_lines.items())
        )

        query = f"""
        SELECT
            file,
            line_start,
            line_end,
            groupArray(test_name) AS tests,
            groupArray(min_depth) AS depths,
            uniqExact(test_name) AS region_test_count
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND ({per_file_conds})
        GROUP BY file, line_start, line_end
        """

        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
        t_query = time.monotonic()
        raw = cidb.query(query, log_level="")
        print(f"[find_tests] CIDB query: {time.monotonic()-t_query:.2f}s, response={len(raw)} bytes")

        # Parse TSV: file \t line_start \t line_end \t [tests] \t [depths] \t region_test_count
        # region_test_count: how many distinct tests cover this region (ownership denominator).
        # Falls back gracefully when columns are absent (old CIDB schema).
        coverage_ranges: list = []
        for row in raw.strip().splitlines():
            if not row:
                continue
            parts = row.split("\t", 5)
            if len(parts) < 4:
                continue
            file_, line_start_s, line_end_s, tests_raw = parts[:4]
            depths_raw = parts[4] if len(parts) >= 5 else None
            count_raw  = parts[5] if len(parts) >= 6 else None
            try:
                line_start = int(line_start_s)
                line_end = int(line_end_s)
                tests = ast.literal_eval(tests_raw.strip())
                depths = ast.literal_eval(depths_raw.strip()) if depths_raw else None
                region_test_count = int(count_raw.strip()) if count_raw else len(tests) if isinstance(tests, list) else 1
                if not isinstance(tests, list):
                    continue
                # Pair each test with its min_depth (255 = not available).
                if isinstance(depths, list) and len(depths) == len(tests):
                    test_depths = [(t, int(d)) for t, d in zip(tests, depths)]
                else:
                    test_depths = [(t, 255) for t in tests]
                coverage_ranges.append((file_, line_start, line_end, test_depths, region_test_count))
            except (ValueError, SyntaxError):
                print(f"Failed to parse coverage row: {row[:100]}")

        # Map each input (filename, line_no) to (test_name, region_width, min_depth,
        # region_test_count) 4-tuples.  The CIDB query returned all ranges for the
        # touched files; now filter to ranges that actually overlap a changed line.
        # Start from base_result which already has empty entries for non-tracked files.
        result: dict = dict(base_result)
        capped_regions = 0
        for filename, line_no in coverage_lines:
            stored = self._stored_path(filename)
            matched: list = []
            for file_, line_start, line_end, test_depths, region_test_count in coverage_ranges:
                if file_ == stored and line_start <= line_no <= line_end:
                    # Skip regions covered by too many tests — these are
                    # infrastructure entry points (e.g. Settings.cpp) that are
                    # exercised by essentially every test and carry no targeting signal.
                    if region_test_count > self.MAX_TESTS_PER_LINE:
                        capped_regions += 1
                        continue
                    width = line_end - line_start + 1
                    for t, depth in test_depths:
                        matched.append((t, width, depth, region_test_count))
            # Deduplicate: keep lowest width, depth, and region_test_count per test.
            by_test: dict = {}  # test -> (min_width, min_depth, min_region_test_count)
            for t, w, d, rc in matched:
                if t not in by_test:
                    by_test[t] = (w, d, rc)
                else:
                    ow, od, orc = by_test[t]
                    by_test[t] = (min(ow, w), min(od, d), min(orc, rc))
            result[(filename, line_no)] = [
                (t, w, d, rc, self.PASS_WEIGHT_DIRECT) for t, (w, d, rc) in sorted(by_test.items())
            ]

        # --- Secondary pass: sibling files in the same source directory ----
        # For each changed C++ file under src/, find tests that cover OTHER files
        # in the same directory.  These tests are added as very broad hits
        # (SIBLING_DIR_WIDTH) so they rank below direct hits but are not silently
        # dropped.  This catches, e.g., Arrow-reader tests when the Arrow writer
        # is changed — the reader and writer live side-by-side and a writer change
        # may break reader round-trips that the direct coverage query misses because
        # those tests never call the writer code path directly.
        sibling_tests = self._query_sibling_dir_tests(files_to_lines, result)
        if sibling_tests:
            # Inject into every changed line so the scorer can accumulate width_score.
            # Use the actual region_test_count from the sibling query so heavily-shared
            # sibling tests are penalised the same way as direct broad hits.
            for key in result:
                fname, _ = key
                stored = self._stored_path(fname)
                dir_path = stored.rsplit("/", 1)[0] + "/"
                if not stored.startswith("./src/"):
                    continue
                for t, rc in sibling_tests.get(dir_path, {}).items():
                    # Skip tests already found by the primary query for this line.
                    existing = {e[0] for e in result[key]}
                    if t not in existing:
                        result[key].append((t, self.SIBLING_DIR_WIDTH, 255, max(1, rc), self.PASS_WEIGHT_SIBLING))


        # --- Tertiary pass: indirect-call (virtual dispatch) co-occurrence ------
        # Find tests calling the same vtable/function-pointer callees as primary
        # tests. Ranked between direct and sibling hits (INDIRECT_CALL_WIDTH=3000).
        indirect_tests = self._query_indirect_call_tests(result)
        if indirect_tests:
            for key in result:
                fname, _ = key
                if not any(fname.startswith(p) for p in COVERAGE_TRACKED_PREFIXES):
                    continue
                existing = {e[0] for e in result[key]}
                for t in indirect_tests:
                    if t not in existing:
                        result[key].append((t, self.INDIRECT_CALL_WIDTH, 255, 1, self.PASS_WEIGHT_INDIRECT))

        total_unique_tests = len({t for pairs in result.values() for t, *_ in pairs})
        lines_with_tests = sum(1 for (f, _), pairs in result.items() if pairs and any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES))
        capped_msg = f", {capped_regions} line-region pairs capped (>{self.MAX_TESTS_PER_LINE} tests)" if capped_regions else ""
        print(
            f"[find_tests] done in {time.monotonic()-t0:.2f}s: "
            f"{lines_with_tests}/{len(coverage_lines)} lines matched, "
            f"{total_unique_tests} unique tests selected{capped_msg}"
        )
        return result

    @staticmethod
    def _extract_domain_keywords(filename: str) -> list:
        """
        Extract domain-specific CamelCase words from a source filename.
        These words are used to filter sibling files so we only expand to
        files in the same functional domain (e.g. "Arrow" for Arrow I/O files)
        rather than all files in the directory.

        Returns a list of significant words (length > 4, not architectural).
        Empty list means no filtering (expand to all sibling files, but this
        is unusual and typically indicates a very generic filename).
        """
        import re as _re
        import os as _os
        base = _os.path.splitext(_os.path.basename(filename))[0]
        # Split CamelCase and all-caps acronyms:
        #   "CHColumnToArrowColumn" → ['CH', 'Column', 'To', 'Arrow', 'Column']
        #   "LDAPClient"            → ['LDAP', 'Client']
        #   "PostgreSQLDictionary"  → ['Postgre', 'SQL', 'Dictionary']
        # Pattern: acronym-run-before-TitleCase | TitleCase-word | lowercase-word
        words = _re.findall(r"[A-Z]+(?=[A-Z][a-z])|[A-Z][a-z0-9]+|[A-Z]{2,}|[a-z][a-z0-9]+", base)
        # Architectural / ubiquitous words that appear in most files in a directory.
        # Keeping this list generous avoids keywords that are too common to be useful.
        COMMON = {
            # Generic C++ / ClickHouse infrastructure words
            "block", "input", "output", "format", "column", "stream",
            "storage", "table", "query", "parser", "writer", "reader",
            "buffer", "default", "base", "impl", "merge", "tree",
            "row", "file", "data", "info", "type", "list", "map",
            "with", "from", "into",
            # MergeTree-specific architectural words (appear in almost every MergeTree file)
            "condition", "granularity", "selector", "partition", "replica",
            "transaction", "virtual", "local", "remote", "range", "level",
            # ClickHouse architectural nouns that appear in many places but are not
            # specific enough to pin to a test domain.
            "handler", "manager", "source", "access", "control",
            "service", "server", "client", "external", "internal",
            "settings", "setting", "config", "context", "result",
            "state", "status", "entry", "record", "update", "create",
        }
        # Lower length threshold to 4 to catch short domain words like "Text", "Avro", "Orc"
        specific = [w for w in words if len(w) >= 4 and w.lower() not in COMMON]
        return specific

    def _query_indirect_call_tests(self, primary_result: dict) -> dict:
        """
        Tertiary pass: find tests that call the same virtual / function-pointer
        callees as the primary tests (those directly covering changed files).

        Semantic: if primary test A covers changed file F and calls virtual
        callee C (recorded via LLVM value profiling), then any other test B
        that also calls callee C is exercising the same interface — even if B
        never directly calls code in F.  This catches tests that reach changed
        implementations only via vtable dispatch or function pointers.

        Uses only the existing `checks_coverage_indirect_calls` CIDB table;
        no new tables are required.  The join is:
          primary tests → their callee_offsets → other tests with same callees.
        The cap (< MAX_TESTS_PER_LINE unique tests per callee) filters out
        ubiquitous callees like `operator new` or `malloc` that every test calls.

        Degrades gracefully when the table is empty (e.g. first nightly run).
        """
        import time

        primary_tests = {t for pairs in primary_result.values() for t, *_ in pairs}
        if not primary_tests:
            return {}

        escaped_primary = ", ".join(
            f"'{self._escape_sql_string(t)}'" for t in sorted(primary_tests)
        )

        # Self-join on callee_offset: find tests that share indirect callees with
        # the primary set, filtered by specificity (< MAX_TESTS_PER_LINE tests
        # per callee to avoid universal callees like allocators).
        query = f"""
        SELECT DISTINCT ic2.test_name
        FROM checks_coverage_indirect_calls ic1
        JOIN checks_coverage_indirect_calls ic2 ON ic1.callee_offset = ic2.callee_offset
        WHERE ic1.check_start_time > now() - interval 3 days
          AND ic2.check_start_time > now() - interval 3 days
          AND ic1.check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND ic2.check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND ic1.test_name IN ({escaped_primary})
          AND ic2.test_name NOT IN ({escaped_primary})
          AND ic1.callee_offset IN (
              SELECT callee_offset
              FROM checks_coverage_indirect_calls
              WHERE check_start_time > now() - interval 3 days
              GROUP BY callee_offset
              HAVING uniqExact(test_name) < {self.MAX_TESTS_PER_LINE}
          )
        LIMIT 200
        """

        try:
            from ci.praktika.cidb import CIDB
            from ci.praktika.settings import Settings
            cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
            t0 = time.monotonic()
            raw = cidb.query(query, log_level="")
            elapsed = time.monotonic() - t0
        except Exception as e:
            print(f"[find_tests] indirect-call query failed (non-fatal): {e}")
            return {}

        tests = [row.strip() for row in raw.strip().splitlines() if row.strip()]
        if not tests:
            return {}

        print(
            f"[find_tests] indirect-call query: {elapsed:.2f}s, "
            f"{len(tests)} additional test candidates via callee co-occurrence"
        )
        # Return as a flat dict {test_name -> INDIRECT_CALL_RC} where RC is the
        # number of shared callees (approximated as 1 here since we SELECT DISTINCT).
        return {t: 1 for t in tests}

    def _query_sibling_dir_tests(self, files_to_lines: dict, primary_result: dict) -> dict:
        """
        Secondary pass: find tests that are likely related to the changed files
        by co-coverage proximity.

        For each changed C++ source file under src/ we:
          1. Identify which sibling files (same directory, different name) the
             PRIMARY tests also cover.  These are files with a natural code
             relationship to the changed file (e.g. Arrow reader ↔ writer).
          2. Find tests that cover those same sibling files but were NOT already
             selected by the primary query.  These are tests that exercise the
             same functional area from a different entry point.

        Because we only expand through files that the primary tests already touch,
        we avoid the false positives that come from querying the whole directory
        (which would mix in unrelated format tests, HTTP tests, etc.).

        Returns {dir_path -> {test_name -> SIBLING_RC}}.
        """
        import time

        # Collect source directories of changed C++ files.
        src_dirs: dict = {}  # dir_path -> list of changed files in that dir
        for f in files_to_lines:
            if f.startswith("./src/") and (f.endswith(".cpp") or f.endswith(".h")):
                dir_path = f.rsplit("/", 1)[0] + "/"
                src_dirs.setdefault(dir_path, []).append(f)

        if not src_dirs:
            return {}

        # Collect primary tests (those already found by the direct-coverage query).
        primary_tests = {t for pairs in primary_result.values() for t, *_ in pairs}
        if not primary_tests:
            return {}

        changed_files = set(files_to_lines.keys())
        escaped_primary = ", ".join(
            f"'{self._escape_sql_string(t)}'" for t in sorted(primary_tests)
        )
        dir_conds = " OR ".join(
            f"file LIKE '{self._escape_sql_string(d)}%'"
            for d in sorted(src_dirs)
        )
        not_changed = " AND ".join(
            f"file != '{self._escape_sql_string(f)}'"
            for f in sorted(changed_files)
        )

        # Extract domain keywords from changed C++ SOURCE filenames only (not
        # test files, which would add misleading keywords like "uuid" or "parquet").
        # E.g. "Arrow" from CHColumnToArrowColumn.cpp ensures we only look at
        # Arrow-related siblings, not every file in the directory.
        all_keywords: list = []
        for f in sorted(changed_files):
            if f.startswith("./src/") and (f.endswith(".cpp") or f.endswith(".h")):
                all_keywords.extend(self._extract_domain_keywords(f.split("/")[-1]))
        # Deduplicate, keep unique keywords only (not repeated across changed files)
        unique_kws = list(dict.fromkeys(all_keywords))

        # Build keyword filter for sibling filenames.
        # With a single keyword (e.g. "Arrow") use LIKE '%Arrow%'.
        # With multiple keywords (e.g. "Index"+"Text") use AND so we only
        # match files containing ALL keywords — this avoids matching broad
        # architectural files that share only one word with the changed file
        # (e.g. MergeTreeIndexGranularity.cpp shares "Index" with
        # MergeTreeIndexConditionText.cpp but is unrelated to text indexing).
        if unique_kws:
            kws = unique_kws[:4]  # cap to avoid overly long queries
            if len(kws) == 1:
                kw_cond = f"file LIKE '%{self._escape_sql_string(kws[0])}%'"
            else:
                # AND: sibling file must contain every keyword
                kw_cond = " AND ".join(
                    f"file LIKE '%{self._escape_sql_string(kw)}%'"
                    for kw in kws
                )
            sibling_file_filter = f"AND ({kw_cond})"
        else:
            # No specific keywords — fall back to all sibling files (rare)
            sibling_file_filter = ""

        # Step 1 + 2 combined: find tests covering sibling files in the same
        # functional domain as the changed file.  The inner SELECT identifies
        # sibling files with the right domain keywords that the primary tests
        # already touch; the outer SELECT finds new tests for those same files.
        query = f"""
        SELECT DISTINCT test_name
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND test_name NOT IN ({escaped_primary})
          AND ({dir_conds})
          AND ({not_changed})
          {sibling_file_filter}
          AND file IN (
              SELECT DISTINCT file
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND test_name IN ({escaped_primary})
                AND ({dir_conds})
                AND ({not_changed})
                {sibling_file_filter}
          )
        LIMIT 200
        """

        try:
            from ci.praktika.cidb import CIDB
            from ci.praktika.settings import Settings
            cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
            t0 = time.monotonic()
            raw = cidb.query(query, log_level="")
            print(
                f"[find_tests] sibling-dir query: {time.monotonic()-t0:.2f}s, "
                f"response={len(raw)} bytes"
            )
        except Exception as e:
            print(f"[find_tests] sibling-dir query failed (non-fatal): {e}")
            return {}

        # Parse TSV: test_name (single column).
        SIBLING_RC = 500
        sibling_test_names: dict = {}
        for row in raw.strip().splitlines():
            test_name = row.strip()
            if test_name:
                sibling_test_names[test_name] = SIBLING_RC

        total = len(sibling_test_names)
        print(f"[find_tests] sibling-dir: {total} additional test candidates")
        # Return the same set for every changed src dir.
        return {d: dict(sibling_test_names) for d in src_dirs}

    # Synthetic width for keyword-fallback tests (broader than sibling-dir tests
    # since we have no coverage signal at all — just filename matching).
    KEYWORD_FALLBACK_WIDTH = 50000

    def _get_keyword_fallback_tests(self, changed_src_files: list) -> list:
        """
        Last-resort fallback: when coverage gives zero results for one or more C++
        source files, try to find stateless tests whose *filename* contains domain
        keywords extracted from the changed source files.

        This handles cases like `Parquet/Decoding.cpp` where the file may not appear
        in the coverage database (e.g. the changed code path is rarely executed by
        existing tests), but there clearly exist stateless tests named
        `*parquet*.sql` / `*parquet*.sh` that should be run.

        Returns a list of `(test_name, width, depth, region_test_count)` tuples
        using KEYWORD_FALLBACK_WIDTH so they always rank below any direct or
        sibling hit.
        """
        import glob as _glob

        if not changed_src_files:
            return []

        # Collect domain keywords from ALL changed source files.
        # Include the *parent directory name* in keyword extraction so that
        # generic filenames like `Decoding.cpp` inside `Parquet/` still yield
        # "Parquet" as a keyword and match `*parquet*.sh` tests.
        all_keywords: list = []
        # Top-level src subdirectories whose names are too generic to use as
        # test-name keywords (they appear in thousands of test files).
        GENERIC_DIRS = frozenset({
            "src", "programs", "utils", "base",
            "Storages", "Interpreters", "Processors", "Functions",
            "Common", "Server", "Parsers", "Analyzer", "Formats",
            "Access", "IO", "Disks", "Columns", "DataTypes", "Core",
            "Databases", "Backups", "Coordination", "Client",
            "Daemon", "Compression", "AggregateFunctions",
            "TableFunctions", "Dictionaries", "QueryPipeline",
            # Sub-directories that are generic groupings (not domain names)
            "Impl", "Sources", "Transforms", "Sinks",
            "Utils", "Tests", "tests",
        })

        dir_keywords: list = []  # keywords from path components (directory names)
        for f in changed_src_files:
            # Keywords from basename (e.g. "CHColumnToArrowColumn.cpp" → "Arrow")
            kws = self._extract_domain_keywords(f.split("/")[-1])
            all_keywords.extend(kws)
            # Keywords from ALL path components that are specific enough.
            # e.g. src/Processors/Formats/Impl/Parquet/Decoding.cpp → "Parquet" from the
            # 4th component (skipping "src", "Processors", "Formats", "Impl").
            parts = f.replace("\\", "/").split("/")
            for part in parts[:-1]:  # all path components except the filename
                if part in GENERIC_DIRS:
                    continue
                part_kws = self._extract_domain_keywords(part + ".cpp")
                for pk in part_kws:
                    if pk not in dir_keywords:
                        dir_keywords.append(pk)
        # Directory keywords are prepended so they are considered first (they tend to
        # be better test-name prefixes than generic filename decompositions).
        for dk in dir_keywords:
            if dk not in all_keywords:
                all_keywords.insert(0, dk)
        unique_kws = list(dict.fromkeys(all_keywords))

        if not unique_kws:
            return []

        # Find the 0_stateless test directory relative to the repo root.
        test_dir = Path("tests/queries/0_stateless")
        if not test_dir.is_dir():
            return []

        # Pre-build list of (sql+sh) test filenames for quick iteration.
        all_test_files = [
            f.name for f in test_dir.iterdir()
            if f.name.endswith(".sql") or f.name.endswith(".sh")
        ]

        # For each candidate keyword, count how many tests it matches.
        # Keywords matching too many tests (too generic) or zero tests
        # (no signal) are discarded.
        # We use two tiers:
        #   Specific tier (1–100 hits): high confidence, domain-specific keyword.
        #     At 100 we include domain words like "Arrow" (54), "Parquet" (99),
        #     "text_index" (86), "Variant" (64) while still excluding broad words.
        #   Broad tier  (101–200 hits): lower confidence, include only if no
        #                               specific keyword is available.
        SPECIFIC_MAX = 100
        BROAD_MAX = 200

        def count_hits(kw_lower: str) -> int:
            return sum(1 for f in all_test_files if kw_lower in f.lower())

        specific_kws = []  # (hits, -len, kw) — few hits, long name preferred
        broad_kws = []
        for kw in unique_kws:
            if len(kw) < 4:
                continue
            hits = count_hits(kw.lower())
            if 1 <= hits <= SPECIFIC_MAX:
                specific_kws.append((hits, -len(kw), kw))
            elif SPECIFIC_MAX < hits <= BROAD_MAX:
                broad_kws.append((hits, -len(kw), kw))
        specific_kws.sort()
        broad_kws.sort()

        # Strategy: prefer directory-origin keywords (they directly name the domain,
        # e.g. "Parquet" from src/.../Parquet/Decoding.cpp).
        # Directory keywords come first in unique_kws (prepended above).
        # We use at most one keyword to keep the test set focused.
        dir_kw_set = set(dir_keywords)

        # Split into directory-origin and filename-origin, each sorted by hit count.
        all_ranked = specific_kws + [(h, l, kw) for h, l, kw in broad_kws]
        all_ranked.sort()  # fewest hits first, longer name preferred

        dir_ranked  = [(h, l, kw) for h, l, kw in all_ranked if kw in  dir_kw_set]
        file_ranked = [(h, l, kw) for h, l, kw in all_ranked if kw not in dir_kw_set]

        if dir_ranked:
            # Use the best directory keyword.  If there is also a specific (≤SPECIFIC_MAX hits)
            # filename keyword, combine them so we narrow the result further.
            candidate_kws = [dir_ranked[0][2]]
            if file_ranked and file_ranked[0][0] <= SPECIFIC_MAX:
                candidate_kws.append(file_ranked[0][2])
        elif file_ranked:
            # No directory keyword available; use at most one filename keyword from the
            # specific tier.  Keywords in the broad tier alone are too noisy.
            file_specific = [x for x in file_ranked if x[0] <= SPECIFIC_MAX]
            if not file_specific:
                return []
            candidate_kws = [file_specific[0][2]]
        else:
            return []

        # Collect test names (without extension) matching ANY of the keywords.
        # We do case-insensitive substring matching on the filename.
        matched_tests: set = set()
        for kw in candidate_kws:
            kw_lower = kw.lower()
            for fname in all_test_files:
                if kw_lower in fname.lower():
                    base = os.path.splitext(fname)[0]
                    matched_tests.add(base + ".")

        if matched_tests:
            print(
                f"[find_tests] keyword-fallback: {len(matched_tests)} tests "
                f"matching keywords {candidate_kws}"
            )
        return [
            (t, self.KEYWORD_FALLBACK_WIDTH, 255, 1, self.PASS_WEIGHT_KEYWORD)
            for t in sorted(matched_tests)
        ]

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
        Always fetches the diff from GitHub using `gh pr diff` so that results
        reflect the actual PR regardless of the local checkout state.
        """
        assert self.info.pr_number > 0, "Find tests by diff applicable for PRs only"
        diff_output = Shell.get_output(
            f"gh pr diff {self.info.pr_number} --repo ClickHouse/ClickHouse"
        )
        changed: list = []
        current_file = None
        for line in diff_output.splitlines():
            if line.startswith("+++ b/"):
                current_file = line[6:]
            elif line.startswith("@@ ") and current_file:
                m = re.search(r"\+(\d+)(?:,(\d+))?", line)
                if m:
                    start = int(m.group(1))
                    count = int(m.group(2)) if m.group(2) is not None else 1
                    for ln in range(start, start + count):
                        changed.append((current_file, ln))
        return changed

    # min_depth stores the raw entry-counter call count (capped at 254; 255 = not tracked).
    # A low call count means the function was called rarely during the test → more specific.
    # Tests where a function was called ≤ this many times get the "direct" tier bonus.
    DIRECT_CALL_MAX_DEPTH = 3

    def get_most_relevant_tests(self):
        """
        1. Gets changed lines from the PR diff.
        2. Queries `checks_coverage_lines` for tests covering those lines.
        3. Ranks tests by a composite score with three tiers:
             - Tier A (best):  narrow-region hit AND shallow call depth (≤ DIRECT_CALL_MAX_DEPTH)
             - Tier B:         narrow-region hit, deep call path
             - Tier C (worst): only broad-region hits
           Within each tier tests are ordered by width_score = sum(1/region_width),
           which rewards tests that cover many of the changed lines through narrow regions.
           The min_depth for each test is the shallowest call depth seen across all
           coverage entries for that test (255 = depth not tracked; treated as deep).
        4. Returns the ranked list and a `Result` with info about the findings.
        """
        changed_lines = self.get_changed_lines_from_diff()
        line_to_tests = self.get_tests_by_changed_lines(changed_lines)

        # Keyword-based fallback: if no coverage results were found for any
        # C++ source file, try to find tests by matching the source filename
        # against stateless test names.  This catches files that are rarely
        # (or never) reached by the current coverage suite — e.g.
        # Parquet/Decoding.cpp when the change is a new edge-case code path.
        # The fallback is only activated when the primary coverage query returned
        # zero tests for a C++ file so it does not dilute high-confidence hits.
        COVERAGE_TRACKED_PREFIXES = ("src/", "programs/", "utils/", "base/")
        cpp_with_zero_coverage = [
            f for f, ln in changed_lines
            if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
            and (f.endswith(".cpp") or f.endswith(".h"))
            and not any(pairs for (ff, _), pairs in line_to_tests.items() if ff == f)
        ]
        # Deduplicate file list.
        cpp_with_zero_coverage = list(dict.fromkeys(cpp_with_zero_coverage))
        if cpp_with_zero_coverage:
            fallback_quads = self._get_keyword_fallback_tests(cpp_with_zero_coverage)
            if fallback_quads:
                # Inject fallback tests into the first line of each zero-coverage
                # C++ file so the scorer sees them without duplication.
                injected: set = set()
                for f in cpp_with_zero_coverage:
                    for (ff, ln), pairs in line_to_tests.items():
                        if ff == f and (ff, ln) not in injected:
                            pairs.extend(fallback_quads)
                            injected.add((ff, ln))
                            break

        # Supplementary keyword pass: even for C++ files WITH direct coverage,
        # add tests whose *filename* contains domain keywords from the changed
        # file.  This catches broad regression tests (e.g. 01273_arrow.sh for
        # CHColumnToArrowColumn.cpp) that exercise the same domain through
        # higher-level call paths not captured in line coverage — the same tests
        # the old symbol-based algo found via checks_coverage_inverted.
        # Uses PASS_WEIGHT_KEYWORD (lowest weight) so they rank below all
        # coverage-backed hits but are still present for the scorer.
        cpp_with_coverage = list(dict.fromkeys(
            f for f, ln in changed_lines
            if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
            and (f.endswith(".cpp") or f.endswith(".h"))
            and f not in cpp_with_zero_coverage
            and any(pairs for (ff, _), pairs in line_to_tests.items() if ff == f)
        ))
        if cpp_with_coverage:
            supplement_quads = self._get_keyword_fallback_tests(cpp_with_coverage)
            if supplement_quads:
                injected_s: set = set()
                for f in cpp_with_coverage:
                    for (ff, ln), pairs in line_to_tests.items():
                        if ff == f and (ff, ln) not in injected_s:
                            existing = {q[0] for q in pairs}
                            pairs.extend(
                                q for q in supplement_quads if q[0] not in existing
                            )
                            injected_s.add((ff, ln))
                            break

        # Accumulate per-test scores across all changed lines.
        # line_to_tests values are lists of
        #   (test_name, region_width, min_depth, region_test_count, pass_weight).
        #
        # Scoring combines four signals:
        #   pass_weight   — per-pass multiplier (1.0 direct, 0.3 indirect, 0.1 sibling, 0.05 keyword)
        #   width         — narrow regions (few lines) are more precise → weight 1/width
        #   region_test_count — regions covered by few tests are more specific → weight 1/region_test_count
        #   min_depth     — low call count means this test specifically exercised this path
        #
        # Final score = sum(pass_weight / (width × region_test_count)) across all matched changed lines.
        width_score: dict = {}    # test -> sum(pass_weight/(width×region_test_count))
        has_narrow_hit: dict = {} # test -> bool: any covering region is narrow AND exclusive
        min_depth_seen: dict = {} # test -> minimum call count across all hits
        for quads in line_to_tests.values():
            for t, width, depth, region_test_count, pass_weight in quads:
                rc = max(1, region_test_count)
                width_score[t] = width_score.get(t, 0.0) + pass_weight / (width * rc)
                has_narrow_hit[t] = has_narrow_hit.get(t, False) or (
                    width <= self.NARROW_REGION_MAX_LINES
                )
                if depth < min_depth_seen.get(t, 256):
                    min_depth_seen[t] = depth

        def sort_key(t):
            narrow = has_narrow_hit[t]
            depth = min_depth_seen.get(t, 255)
            direct = narrow and depth <= self.DIRECT_CALL_MAX_DEPTH
            # Tier A: direct narrow hit → 0; Tier B: indirect narrow → 1; Tier C: broad → 2
            tier = 0 if direct else (1 if narrow else 2)
            return (tier, -width_score[t])

        # Sort: tier first (A < B < C), then by width score descending within tier.
        ranked = sorted(width_score, key=sort_key)[:1000]

        narrow_count = sum(1 for t in ranked if has_narrow_hit[t])
        direct_count = sum(
            1 for t in ranked
            if has_narrow_hit[t] and min_depth_seen.get(t, 255) <= self.DIRECT_CALL_MAX_DEPTH
        )
        broad_count = len(ranked) - narrow_count

        info = "Tests found for lines:\n"
        if not line_to_tests:
            info += "  No changed lines found in diff\n"
        else:
            for (file_, line_), pairs in line_to_tests.items():
                if pairs:
                    info += f"  {file_}:{line_} -> {len(pairs)} tests\n"
        info += (
            f"Total unique tests: {len(ranked)} "
            f"({direct_count} direct-narrow, {narrow_count - direct_count} indirect-narrow, "
            f"{broad_count} broad)\n"
        )
        if ranked:
            top = ranked[0]
            d = min_depth_seen.get(top, 255)
            tier = ("direct-narrow" if has_narrow_hit[top] and d <= self.DIRECT_CALL_MAX_DEPTH
                    else "narrow" if has_narrow_hit[top] else "broad")
            info += f"Top test: {top} (score={width_score[top]:.3f}, {tier}, depth={d})\n"

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

    # Run the full ranking pipeline once (avoids double CIDB query).
    # This includes: changed test detection, keyword fallback, sibling expansion.
    changed_lines = targeting.get_changed_lines_from_diff()
    line_to_tests = targeting.get_tests_by_changed_lines(changed_lines)

    # Detect changed test files directly — same as the in-CI path.
    changed_test_files = targeting.get_changed_tests()
    if changed_test_files:
        print(f"\nChanged test files ({len(changed_test_files)}):")
        for t in changed_test_files:
            print(f"  {t}")

    print("\nNo tests found for lines:")
    for (file, line), pairs in line_to_tests.items():
        if pairs:
            continue
        print(f"{file}:{line} -> NOT FOUND")

    # Apply the keyword fallback for zero-coverage C++ files (same logic as
    # get_most_relevant_tests, but reusing the already-fetched line_to_tests).
    COVERAGE_TRACKED_PREFIXES = ("src/", "programs/", "utils/", "base/")
    cpp_with_zero_coverage = list(dict.fromkeys(
        f for f, ln in changed_lines
        if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
        and (f.endswith(".cpp") or f.endswith(".h"))
        and not any(pairs for (ff, _), pairs in line_to_tests.items() if ff == f)
    ))
    fallback_quads: list = []
    if cpp_with_zero_coverage:
        fallback_quads = targeting._get_keyword_fallback_tests(cpp_with_zero_coverage)
        if fallback_quads:
            injected: set = set()
            for f in cpp_with_zero_coverage:
                for (ff, ln), pairs in line_to_tests.items():
                    if ff == f and (ff, ln) not in injected:
                        pairs.extend(fallback_quads)
                        injected.add((ff, ln))
                        break

    # Supplementary keyword pass for C++ files WITH coverage (mirrors get_most_relevant_tests).
    cpp_with_coverage = list(dict.fromkeys(
        f for f, ln in changed_lines
        if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
        and (f.endswith(".cpp") or f.endswith(".h"))
        and f not in cpp_with_zero_coverage
        and any(pairs for (ff, _), pairs in line_to_tests.items() if ff == f)
    ))
    if cpp_with_coverage:
        supplement_quads = targeting._get_keyword_fallback_tests(cpp_with_coverage)
        if supplement_quads:
            injected_s: set = set()
            for f in cpp_with_coverage:
                for (ff, ln), pairs in line_to_tests.items():
                    if ff == f and (ff, ln) not in injected_s:
                        existing = {q[0] for q in pairs}
                        pairs.extend(q for q in supplement_quads if q[0] not in existing)
                        injected_s.add((ff, ln))
                        break

    print("\nTests found for lines:")
    for (file, line), quads in line_to_tests.items():
        if not quads:
            continue
        print(f"{file}:{line}:")
        for test, width, depth, rc, pw in quads:
            narrow_tag = "narrow" if width <= Targeting.NARROW_REGION_MAX_LINES else f"width={width}"
            depth_tag = f"depth={depth}" if depth < 255 else "depth=?"
            print(f" - {test}  [{narrow_tag}, {depth_tag}, pw={pw}]")

    # quads is list of (test_name, region_width, min_depth, region_test_count, pass_weight)
    all_tests: dict = {}  # test -> (min_width, min_depth, min_region_test_count)

    # Seed with changed test files using perfect-signal sentinel values.
    for t in changed_test_files:
        all_tests[t] = (1, 0, 1)  # width=1, depth=0, rc=1 → always top-ranked

    for quads in line_to_tests.values():
        for t, w, d, rc, _pw in quads:
            if t not in all_tests:
                all_tests[t] = (w, d, rc)
            else:
                ow, od, orc = all_tests[t]
                all_tests[t] = (min(ow, w), min(od, d), min(orc, rc))

    print(f"\nAll selected tests ({len(all_tests)}):")
    for test in sorted(all_tests):
        w, d, rc = all_tests[test]
        narrow_tag = "narrow" if w <= Targeting.NARROW_REGION_MAX_LINES else f"width={w}"
        depth_tag = f"depth={d}" if d < 255 else "depth=?"
        print(f" {test}  [{narrow_tag}, {depth_tag}, tests={rc}]")
