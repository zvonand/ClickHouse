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

    # Regions wider than this are considered "broad" (low signal).
    NARROW_REGION_MAX_LINES = 20

    # Regions covered by more tests than this are skipped from Pass 1 (direct line
    # coverage).  Infrastructure files like SignalHandlers.cpp, Context.cpp, and
    # Settings.cpp are touched by almost every test — a changed line in such a file
    # has no diagnostic value for test selection.  Skipping regions with too many
    # owners prevents these files from flooding primary_tests (which would then
    # cause sibling/indirect queries to fail with HTTP form-field-too-long errors
    # and inflate unique_tests to nearly the full suite).
    MAX_TESTS_PER_LINE = 200

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
    PASS_WEIGHT_BROAD2   = 0.15  # Pass 4: test covers changed files via very-broad regions (rc 2001-8000)
    PASS_WEIGHT_SIBLING  = 0.1   # Pass 2: test covers a sibling file in the same source directory
    PASS_WEIGHT_KEYWORD  = 0.05  # Fallback: test filename contains domain keywords from changed files

    def get_tests_by_changed_lines(self, changed_lines: list) -> dict:
        """
        Query `checks_coverage_lines` for tests that cover each (filename, line_no) pair.

        `changed_lines` is a list of `(filename, line_no)` tuples.
        Returns a dict mapping each input tuple to a list of `(test_name, region_width)`
        tuples, where `region_width = line_end - line_start + 1`.  The region width is
        used by the caller to weight test scores (narrow regions = high signal).

        All matching regions are included; the scoring formula naturally penalises
        regions covered by many tests via the `region_test_count` denominator.
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
        #
        # For header files (.h), we expand the line range to the entire file because
        # headers typically define a single class and a change to any method affects
        # the class semantics.  Tests covering OTHER methods in the same header are
        # highly relevant.  This mirrors the old symbol-level algo which matched all
        # symbols in the same translation unit.
        per_file_conds_parts = []
        for f, lines in sorted(files_to_lines.items()):
            if f.endswith(".h"):
                # For headers: fetch ALL regions in the file (no line range filter)
                per_file_conds_parts.append(
                    f"(file = '{self._escape_sql_string(f)}')"
                )
            else:
                per_file_conds_parts.append(
                    f"(file = '{self._escape_sql_string(f)}'"
                    f" AND line_end >= {min(lines)} AND line_start <= {max(lines)})"
                )
        per_file_conds = " OR ".join(per_file_conds_parts)

        # Two-tier query: primary (narrow, <= MAX_TESTS_PER_LINE tests per
        # region) and broad (> MAX, <= BROAD_REGION_HARD_CAP).  Broad regions
        # are included so infrastructure files (Context.cpp, RemoteQueryExecutor,
        # etc.) are not completely invisible — the scoring formula already
        # penalises them via the 1/region_test_count denominator.  Only truly
        # ubiquitous regions (> HARD_CAP) are dropped to keep response size sane.
        BROAD_REGION_HARD_CAP = 2000  # scoring handles broad regions; very ubiquitous dropped
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
        HAVING region_test_count <= {BROAD_REGION_HARD_CAP}
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

        # --- Second tier: very broad regions (rc > BROAD_REGION_HARD_CAP) --------
        # Infrastructure files (Context.cpp, ProcessList.cpp) have regions covered
        # by 3000-8000+ tests.  These are dropped by the primary query to keep its
        # response size manageable (groupArray would return thousands of names per
        # region).  However, the tests covering these broad regions are still
        # relevant — they just have low specificity.
        #
        # The second-tier query returns only DISTINCT test names (no per-region
        # grouping) for regions above the primary cap.  This keeps the response
        # size linear in the number of unique tests rather than quadratic in
        # (regions × tests_per_region).  Tests found this way are assigned the
        # BROAD_FALLBACK_WIDTH and a synthetic region_test_count equal to the
        # average rc across all matching broad regions.
        # Skip the broad-tier2 query if we've already consumed too much time.
        elapsed_after_primary = time.monotonic() - t0
        run_broad_tier2 = elapsed_after_primary < 8.0
        if not run_broad_tier2:
            print(f"[find_tests] skipping broad-tier2 query (elapsed={elapsed_after_primary:.1f}s)")

        VERY_BROAD_REGION_CAP = 8000  # drop truly ubiquitous regions
        # Query broad regions and count how many regions each test covers in the
        # changed files.  Tests covering more regions get proportionally higher
        # scores, so tests that genuinely exercise the changed code path (even via
        # broad Context.cpp/ProcessList.cpp regions) rank above tests that merely
        # touch one broad region.  ORDER BY cov_regions DESC + LIMIT ensures we
        # prioritise the most-covering tests when truncating.
        broad_query = f"""
        SELECT test_name, count() AS cov_regions
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND ({per_file_conds})
          AND (file, line_start, line_end) IN (
              SELECT file, line_start, line_end
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                AND ({per_file_conds})
              GROUP BY file, line_start, line_end
              HAVING uniqExact(test_name) > {BROAD_REGION_HARD_CAP}
                 AND uniqExact(test_name) <= {VERY_BROAD_REGION_CAP}
          )
        GROUP BY test_name
        ORDER BY cov_regions DESC
        LIMIT 8000
        """
        # Broad-tier2 tests have real coverage but from regions shared by
        # thousands of tests.  The effective score is:
        #   cov_regions × PASS_WEIGHT_BROAD_TIER2 / (BROAD_FALLBACK_WIDTH × BROAD_TIER2_RC)
        # = cov_regions × 2e-7
        # A test covering 100 broad regions scores 2e-5 (much higher than the
        # flat 2e-7 the old single-injection approach gave every test).
        BROAD_FALLBACK_WIDTH = 500  # synthetic width — broad but real coverage
        # Synthetic region_test_count: set so that PASS_WEIGHT_BROAD2 / (BROAD_FALLBACK_WIDTH
        # × BROAD_TIER2_RC) stays in the same absolute score range as before while correctly
        # reflecting that broad-tier2 signal is weaker than indirect-call (0.15 < 0.30).
        # With PASS_WEIGHT_BROAD2=0.15 and the same score target (1e-5 at cov_regions=50,
        # effective_width=10): BROAD_TIER2_RC = 0.15 / (10 × 1e-5) = 1500.
        BROAD_TIER2_RC = 1500
        PASS_WEIGHT_BROAD_TIER2 = self.PASS_WEIGHT_BROAD2

        broad_tests: dict = {}   # test_name -> cov_regions count
        if run_broad_tier2:
            t_broad = time.monotonic()
            try:
                broad_raw = cidb.query(broad_query, log_level="")
                broad_elapsed = time.monotonic() - t_broad
                print(f"[find_tests] broad-tier2 query: {broad_elapsed:.2f}s, response={len(broad_raw)} bytes")
            except Exception as e:
                print(f"[find_tests] broad-tier2 query failed (non-fatal): {e}")
                broad_raw = ""

            # Parse broad-tier2 results: test_name \t cov_regions
            for row in broad_raw.strip().splitlines():
                parts = row.strip().split("\t", 1)
                if parts[0]:
                    count = int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else 1
                    broad_tests[parts[0]] = count
            if broad_tests:
                max_cov = max(broad_tests.values()) if broad_tests else 0
                print(
                    f"[find_tests] broad-tier2: {len(broad_tests)} additional tests from "
                    f"regions with {BROAD_REGION_HARD_CAP} < rc <= {VERY_BROAD_REGION_CAP} "
                    f"(top cov_regions={max_cov})"
                )

        # Map each input (filename, line_no) to (test_name, region_width, min_depth,
        # region_test_count) 4-tuples.  The CIDB query returned all ranges for the
        # touched files; now filter to ranges that actually overlap a changed line.
        # Start from base_result which already has empty entries for non-tracked files.
        # Pre-index coverage_ranges by file for faster lookup.
        ranges_by_file: dict = {}
        for entry in coverage_ranges:
            ranges_by_file.setdefault(entry[0], []).append(entry)

        result: dict = dict(base_result)
        for filename, line_no in coverage_lines:
            stored = self._stored_path(filename)
            matched: list = []
            is_header = filename.endswith(".h")
            for file_, line_start, line_end, test_depths, region_test_count in ranges_by_file.get(stored, []):
                overlaps = line_start <= line_no <= line_end
                if overlaps:
                    width = line_end - line_start + 1
                    for t, depth in test_depths:
                        matched.append((t, width, depth, region_test_count))
                elif is_header:
                    # For header files: include non-overlapping regions from
                    # the same file with SIBLING_DIR_WIDTH penalty.  These are
                    # other methods in the same class, still relevant but with
                    # weaker signal than a direct line overlap.
                    for t, depth in test_depths:
                        matched.append((t, self.SIBLING_DIR_WIDTH, 255, region_test_count))
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

        # Inject broad-tier2 tests into coverage results.
        # Tests are injected into the first coverage-tracked changed line only.
        # The effective width is inversely proportional to cov_regions so that
        # tests covering more of the changed files get lower width → higher score:
        #   score = PASS_WEIGHT_BROAD_TIER2 / (effective_width × BROAD_TIER2_RC)
        #         = 0.5 / (max(1, 500//cov_regions) × 5000)
        # A test covering 100 regions: effective_width=5, score=0.5/(5×5000)=2e-5
        # A test covering   1 region:  effective_width=500, score=0.5/(500×5000)=2e-7
        # The 100x difference correctly reflects relative specificity.
        if broad_tests:
            for filename, line_no in coverage_lines:
                if not any(filename.startswith(p) for p in COVERAGE_TRACKED_PREFIXES):
                    continue
                key = (filename, line_no)
                existing = {e[0] for e in result.get(key, [])}
                for tname, cov_regions in broad_tests.items():
                    if tname not in existing:
                        effective_width = max(1, BROAD_FALLBACK_WIDTH // max(1, cov_regions))
                        result.setdefault(key, []).append(
                            (tname, effective_width, 255, BROAD_TIER2_RC, PASS_WEIGHT_BROAD_TIER2)
                        )
                break  # inject into first tracked line only (score is cov_regions-weighted)

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
                for t, shared_count in indirect_tests.items():
                    if t not in existing:
                        # Scale effective width inversely with shared_callees so that
                        # tests sharing more callees with the primary set rank higher.
                        # Floor at NARROW_REGION_MAX_LINES+1 to stay in the broad tier.
                        effective_width = max(
                            self.NARROW_REGION_MAX_LINES + 1,
                            self.INDIRECT_CALL_WIDTH // max(1, shared_count),
                        )
                        result[key].append((t, effective_width, 255, 1, self.PASS_WEIGHT_INDIRECT))

        total_unique_tests = len({t for pairs in result.values() for t, *_ in pairs})
        lines_with_tests = sum(1 for (f, _), pairs in result.items() if pairs and any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES))
        print(
            f"[find_tests] done in {time.monotonic()-t0:.2f}s: "
            f"{lines_with_tests}/{len(coverage_lines)} lines matched, "
            f"{total_unique_tests} unique tests selected"
        )

        # Store top broad-tier2 tests (by cov_regions) on self so that
        # get_most_relevant_tests() can guarantee they make it through
        # the MAX_OUTPUT_TESTS cap even if their scores fall below the cutoff.
        n_guarantee = min(50, max(10, len(coverage_lines) // 5))
        self._broad_tier2_guarantee = [
            t
            for t, _ in sorted(broad_tests.items(), key=lambda x: -x[1])
        ][:n_guarantee] if broad_tests else []

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
        # The trailing [A-Z] alternative captures single uppercase chars that
        # the other patterns miss (e.g. the "K" in "TopK" or "N" in "MergeN").
        words = _re.findall(r"[A-Z]+(?=[A-Z][a-z])|[A-Z][a-z0-9]+|[A-Z]{2,}|[a-z][a-z0-9]+|[A-Z]", base)
        # Merge a lone trailing uppercase letter into the previous word so that
        # compound names like "TopK" or "MergeN" are kept whole instead of losing
        # the suffix.
        merged: list = []
        for w in words:
            if len(w) == 1 and w.isupper() and merged:
                merged[-1] = merged[-1] + w
            else:
                merged.append(w)
        words = merged
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
        # Allow 3-char all-uppercase acronyms (CSV, ORC, URL, JWT, KQL, etc.) in addition
        # to words ≥ 4 chars.  Generic acronyms like "API", "SQL", "DDL", "DML" are added
        # to COMMON below so they don't generate false matches.
        COMMON_ACRONYMS = {"api", "sql", "ddl", "dml", "ids", "uid", "abi", "cpu", "gpu", "ram",
                           "tcp", "udp", "tls", "ssl", "rpc", "ttl", "log", "tag", "row", "set"}
        specific = [
            w for w in words
            if w.lower() not in COMMON
            and (
                (len(w) >= 4)
                or (len(w) == 3 and w.isupper() and w.lower() not in COMMON_ACRONYMS)
            )
        ]
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
        Shared-library callees (operator new, malloc, etc.) are excluded at
        collection time in coverage.cpp, so no extra filter is needed here.

        Degrades gracefully when the table is empty (e.g. first nightly run).
        """
        import time

        # Use only high-confidence primary tests (from regions with
        # <= MAX_TESTS_PER_LINE) for the indirect-call join.  Including all
        # tests from broad regions (e.g. 1000+) would make the IN-list too
        # large for the CIDB HTTP endpoint ("Field value too long").
        primary_tests = set()
        for pairs in primary_result.values():
            for entry in pairs:
                t = entry[0]
                rc = entry[3] if len(entry) > 3 else 1
                pw = entry[4] if len(entry) > 4 else 1.0
                if rc <= self.MAX_TESTS_PER_LINE and pw >= self.PASS_WEIGHT_INDIRECT:
                    primary_tests.add(t)
        if not primary_tests:
            return {}

        # Cap at 500 tests to prevent HTTP field-length errors.
        if len(primary_tests) > 500:
            primary_tests = set(sorted(primary_tests)[:500])

        escaped_primary = ", ".join(
            f"'{self._escape_sql_string(t)}'" for t in sorted(primary_tests)
        )

        # Self-join on callee_offset ranked by Jaccard-like specificity.
        #
        # Problem with raw shared_callees ordering: infrastructure tests (filesystem
        # cache, S3) rank highest because they share many I/O virtual-dispatch callees
        # with any MergeTree-reading primary test, even if they're completely unrelated
        # to the changed code domain.
        #
        # Fix: rank by  shared / secondary_test_total_specific_callees  (Jaccard
        # fraction).  A test whose callee set is mostly overlapping with the primary
        # set is specifically exercising the same domain; a filesystem-cache test whose
        # 200 specific callees all happen to be in the primary set gets lower rank than
        # a DDL test whose 50 specific callees are 90% shared with the primary set.
        #
        # Additional guards:
        #   MIN_SHARED       — require at least this many shared callees (eliminates
        #                      accidental 1-2 callee overlaps).
        #   MIN_SECONDARY    — secondary test must have at least this many specific
        #                      callees; avoids 100%-Jaccard artifacts from tiny sets.
        #   MAX_CALLEE_COUNT — exclude globally-ubiquitous callees (logging, malloc).
        MAX_CALLEE_TEST_COUNT = 200  # callees in >= this many tests are ubiquitous
        # Thresholds scale with the primary set size so that small primary sets
        # (e.g. 3 tests for a focused single-file PR) still find results while
        # large primary sets use stricter filters to suppress infrastructure noise.
        n_primary = len(primary_tests)
        MIN_SHARED    = max(3, min(10, n_primary // 5))   # 3 for tiny, 10 for large
        MIN_SECONDARY = max(20, min(50, n_primary * 3))   # 20 for tiny, 50 for large
        query = f"""
        SELECT
            ic2.test_name,
            count(DISTINCT ic1.callee_offset) AS shared_callees,
            ic2_tot.tot_callees,
            count(DISTINCT ic1.callee_offset) * 100.0 / ic2_tot.tot_callees AS jaccard_pct
        FROM checks_coverage_indirect_calls ic1
        JOIN checks_coverage_indirect_calls ic2 ON ic1.callee_offset = ic2.callee_offset
        JOIN (
            -- Total number of specific (non-ubiquitous) callees each secondary test
            -- uses.  Used as the Jaccard denominator.
            SELECT test_name, count(DISTINCT callee_offset) AS tot_callees
            FROM checks_coverage_indirect_calls
            WHERE check_start_time > now() - interval 3 days
              AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              AND callee_offset IN (
                  SELECT callee_offset
                  FROM checks_coverage_indirect_calls
                  WHERE check_start_time > now() - interval 3 days
                    AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                  GROUP BY callee_offset
                  HAVING uniqExact(test_name) < {MAX_CALLEE_TEST_COUNT}
              )
            GROUP BY test_name
            HAVING tot_callees >= {MIN_SECONDARY}
        ) ic2_tot ON ic2.test_name = ic2_tot.test_name
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
                AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              GROUP BY callee_offset
              HAVING uniqExact(test_name) < {MAX_CALLEE_TEST_COUNT}
          )
        GROUP BY ic2.test_name, ic2_tot.tot_callees
        HAVING shared_callees >= {MIN_SHARED}
        ORDER BY jaccard_pct DESC, shared_callees DESC
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

        # Parse TSV: test_name \t shared_callees \t tot_callees \t jaccard_pct
        result_map: dict = {}
        for row in raw.strip().splitlines():
            parts = row.split("\t")
            if len(parts) >= 2 and parts[0].strip():
                try:
                    result_map[parts[0].strip()] = int(parts[1].strip())
                except ValueError:
                    pass
        if not result_map:
            return {}

        # Extract top Jaccard score from the raw output for logging
        top_jaccard = 0.0
        for row in raw.strip().splitlines():
            parts = row.split("\t")
            if len(parts) >= 4:
                try:
                    top_jaccard = max(top_jaccard, float(parts[3].strip()))
                except ValueError:
                    pass
        print(
            f"[find_tests] indirect-call query: {elapsed:.2f}s, "
            f"{len(result_map)} additional test candidates via callee co-occurrence "
            f"(top jaccard={top_jaccard:.0f}%, top shared={max(result_map.values())})"
        )
        return result_map

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

        # Collect high-confidence primary tests (from narrow or moderately-broad
        # regions) for the sibling-dir expansion.  Using all primary tests
        # (including those from very broad regions) would make the IN-list
        # too large for the CIDB HTTP endpoint.
        primary_tests = set()
        for pairs in primary_result.values():
            for entry in pairs:
                t = entry[0]
                rc = entry[3] if len(entry) > 3 else 1
                pw = entry[4] if len(entry) > 4 else 1.0
                if rc <= self.MAX_TESTS_PER_LINE and pw >= self.PASS_WEIGHT_INDIRECT:
                    primary_tests.add(t)
        if not primary_tests:
            return {}

        # Cap at 500 tests to prevent HTTP field-length errors.
        if len(primary_tests) > 500:
            primary_tests = set(sorted(primary_tests)[:500])

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
        #
        # Two filters prevent broad infrastructure files from flooding the candidates:
        #
        # 1. INNER subquery min coverage: the sibling file must be covered by at
        #    least MIN_SIBLING_COVERAGE primary tests.  This excludes files that are
        #    only incidentally touched by primary tests (e.g. AggregatedDataVariants.cpp
        #    being exercised by Variant tests through GROUP BY operations).
        #
        # 2. Global test count cap (NOT IN subquery): exclude sibling files that are
        #    covered by more than MAX_SIBLING_TESTS distinct tests globally.  Very
        #    broadly-covered files (like AggregatedDataVariants.cpp with 3400 tests,
        #    or RewriteCountVariantsVisitor.cpp with 4250 tests) are infrastructure —
        #    finding their tests adds noise rather than signal.  The threshold matches
        #    MAX_TESTS_PER_LINE so we consistently exclude "too common" files.
        MAX_SIBLING_FILE_TESTS = self.MAX_TESTS_PER_LINE  # same cap as direct coverage
        n_primary = len(primary_tests)
        min_sibling_coverage = max(2, n_primary // 5)
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
          AND file NOT IN (
              SELECT file
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                AND ({dir_conds})
                {sibling_file_filter}
              GROUP BY file
              HAVING uniqExact(test_name) > {MAX_SIBLING_FILE_TESTS}
          )
          AND file IN (
              SELECT file
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND test_name IN ({escaped_primary})
                AND ({dir_conds})
                AND ({not_changed})
                {sibling_file_filter}
              GROUP BY file
              HAVING uniqExact(test_name) >= {min_sibling_coverage}
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

        # Pre-count actual test-file hits so we can detect zero-hit keywords and skip them.
        def has_hits(kw: str) -> bool:
            kw_lower = kw.lower()
            return any(kw_lower in f.lower() for f in all_test_files)

        if dir_ranked:
            # Use the best directory keyword.  Skip any that match zero tests (e.g.
            # "Coordination" for KeeperStorage.cpp — no test names contain "coordination").
            # Fall back through the list until one has hits; if none do, continue to
            # file_ranked below.
            usable_dir = [(h, l, kw) for h, l, kw in dir_ranked if has_hits(kw)]
            if usable_dir:
                candidate_kws = [usable_dir[0][2]]
                # Also add a specific file keyword to narrow results further.
                if file_ranked and file_ranked[0][0] <= SPECIFIC_MAX:
                    candidate_kws.append(file_ranked[0][2])
            else:
                # All directory keywords have 0 test hits; fall through to file_ranked.
                dir_ranked = []

        if not dir_ranked:
            if not file_ranked:
                return []
            # No useful directory keyword; use the most specific file keyword available.
            # Prefer the specific tier (≤SPECIFIC_MAX), but fall back to the broad tier
            # (≤BROAD_MAX) when nothing specific exists — e.g. "Keeper" (156 hits) is
            # still a meaningful domain signal for KeeperStorage.cpp changes.
            file_specific = [x for x in file_ranked if x[0] <= SPECIFIC_MAX]
            if file_specific:
                candidate_kws = [file_specific[0][2]]
            elif broad_kws:
                candidate_kws = [broad_kws[0][2]]
            else:
                return []

        # Collect test names matching any of the selected keywords.
        # Track the best (most specific) keyword that matched each test so we
        # can weight by keyword specificity: rare keywords → narrow effective
        # width → higher score.  Among ties, prefer newer tests (higher number).
        #
        # Scoring:  score = PASS_WEIGHT_KEYWORD / (KEYWORD_FALLBACK_WIDTH × kw_hits)
        #   kw_hits=1  → width=50000   score=1e-6  (very specific)
        #   kw_hits=10 → width=500000  score=1e-7
        #   kw_hits=100→ width=5000000 score=1e-8  ≈ MIN_SCORE (barely survives)
        # Tests from broad keywords (hits>100) score below MIN_SCORE and are
        # dropped at the ranking stage — no hard per-keyword filter needed.
        MAX_KEYWORD_TESTS = 30   # hard cap: keyword signal is weak, keep it tight
        matched_tests: dict = {}  # normalised_name → (kw_hits, raw_name)
        for kw in candidate_kws:
            kw_lower = kw.lower()
            kw_hits = count_hits(kw_lower)
            for fname in all_test_files:
                if kw_lower in fname.lower():
                    base = os.path.splitext(fname)[0]
                    tname = base + "."
                    if tname not in matched_tests or kw_hits < matched_tests[tname][0]:
                        matched_tests[tname] = (kw_hits, kw)

        if not matched_tests:
            return []

        # Sort: most-specific keyword first (fewest hits), then newest test first
        # (higher 5-digit prefix tends to be closer to the recently changed code).
        def _kw_sort(item):
            tname, (kw_hits, _kw) = item
            m = re.match(r'(\d{5})', os.path.basename(tname).lstrip('./'))
            test_num = int(m.group(1)) if m else 0
            return (kw_hits, -test_num)

        sorted_tests = sorted(matched_tests.items(), key=_kw_sort)[:MAX_KEYWORD_TESTS]
        kws_used = sorted({kw for _, (_, kw) in sorted_tests})
        print(
            f"[find_tests] keyword-fallback: {len(sorted_tests)} tests "
            f"matching keywords {kws_used} (from {len(matched_tests)} candidates)"
        )
        return [
            (tname, self.KEYWORD_FALLBACK_WIDTH * kw_hits, 255, 1, self.PASS_WEIGHT_KEYWORD)
            for tname, (kw_hits, _kw) in sorted_tests
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
        try:
            tests = self.get_previously_failed_tests()
        except Exception as e:
            print(
                f"WARNING: Failed to get previously failed tests (best effort): {e}",
                file=sys.stderr,
            )
            tests = []
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
            # Run per-file so each file's domain keywords are matched independently.
            seen_fb: set = set()
            fallback_quads: list = []
            for f in cpp_with_zero_coverage:
                for q in self._get_keyword_fallback_tests([f]):
                    if q[0] not in seen_fb:
                        seen_fb.add(q[0]); fallback_quads.append(q)
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
            # Run keyword fallback per changed file so each file's domain keywords
            # are matched independently.  Calling with all files at once collapses
            # to a single "best" keyword and silently drops domains from other files
            # (e.g. for a PR touching ArrowColumnToCHColumn.cpp + MsgPackRowInputFormat.cpp
            # the combined call picks "Pack" as more specific, missing all Arrow tests).
            seen_supplement: set = set()
            all_supplement_quads: list = []
            for f in cpp_with_coverage:
                per_file_quads = self._get_keyword_fallback_tests([f])
                for q in per_file_quads:
                    if q[0] not in seen_supplement:
                        seen_supplement.add(q[0])
                        all_supplement_quads.append(q)
            if all_supplement_quads:
                injected_s: set = set()
                for f in cpp_with_coverage:
                    for (ff, ln), pairs in line_to_tests.items():
                        if ff == f and (ff, ln) not in injected_s:
                            existing = {q[0] for q in pairs}
                            pairs.extend(
                                q for q in all_supplement_quads if q[0] not in existing
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
        # Use score-based filtering instead of a hard count cap.  The old
        # `[:1000]` cut threw away low-scoring but genuinely relevant tests
        # from infrastructure files (Context.cpp, ProcessList.cpp) whose
        # regions have high region_test_count and therefore low per-line
        # scores.  A minimum score threshold keeps the result set bounded
        # without an arbitrary count limit.
        MIN_SCORE = 1e-8        # floor: tests scoring below this have negligible signal
        MAX_OUTPUT_TESTS = 500   # hard cap: targeted runs must stay focused
        all_ranked = sorted(width_score, key=sort_key)
        ranked = [t for t in all_ranked if width_score[t] >= MIN_SCORE][:MAX_OUTPUT_TESTS]

        # Broad-tier2 guarantee: if the cap cut off high-cov_regions broad-tier2 tests,
        # append the top few (by cov_regions) that didn't make it — but only up to the cap.
        broad_guarantee = getattr(self, '_broad_tier2_guarantee', [])
        if broad_guarantee and len(ranked) < MAX_OUTPUT_TESTS:
            ranked_set = set(ranked)
            slots = MAX_OUTPUT_TESTS - len(ranked)
            extra = [t for t in broad_guarantee if t not in ranked_set][:slots]
            if extra:
                ranked = ranked + extra
                print(f"[find_tests] broad-tier2 guarantee: +{len(extra)} high-cov_regions tests added")

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
        scored_total = len(width_score)
        filtered_out = scored_total - len(ranked)
        info += (
            f"Total unique tests: {len(ranked)} "
            f"({direct_count} direct-narrow, {narrow_count - direct_count} indirect-narrow, "
            f"{broad_count} broad"
        )
        if filtered_out > 0:
            info += f"; {filtered_out} below score threshold"
        info += ")\n"
        if ranked:
            top = ranked[0]
            d = min_depth_seen.get(top, 255)
            tier = ("direct-narrow" if has_narrow_hit[top] and d <= self.DIRECT_CALL_MAX_DEPTH
                    else "narrow" if has_narrow_hit[top] else "broad")
            info += f"Top test: {top} (score={width_score[top]:.6f}, {tier}, depth={d})\n"
            bottom = ranked[-1]
            info += f"Bottom test: {bottom} (score={width_score[bottom]:.6f})\n"

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
    # local run: use the same pipeline as CI (get_all_relevant_tests_with_info)
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

    ranked, result = targeting.get_all_relevant_tests_with_info()

    print(f"\nAll selected tests ({len(ranked)}):")
    for test in ranked:
        print(f" {test}")
    print(f"\n{result.info}")
