import argparse
import ast
import os
import re
import sys
from pathlib import Path

sys.path.append("./")

from ci.jobs.scripts.find_symbols import DiffToSymbols
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
    def normalize_symbol(symbol: str) -> str:
        """
        Extract the qualified function name from a demangled C++ symbol by:
          1. Finding the first '(' at bracket depth 0 to locate the arg list
          2. Finding the last ' ' at bracket depth 0 before that to strip return type
          3. Stripping trailing function template args '<...>' if the result ends with '>'

        Handles all cases: 'void DB::Foo::bar()', 'std::shared_ptr<DB::Type> DB::Foo::bar()',
        '(anonymous namespace)::DistributedIndexAnalyzer::method()',
        'void DB::JoinStuff::JoinUsedFlags::setUsed<true, true>'

        This is the query-time counterpart of the SQL normalization in export_coverage.py.
        hasAllTokens() is robust to return-type tokens in stored symbols, so the stored
        symbol doesn't need identical normalization — only the function name tokens matter.
        """
        # Replace (anonymous namespace) with same-length placeholder so its '(' and ')'
        # don't confuse the depth tracking or the arg-list '(' detection.
        ANON = "(anonymous namespace)"
        modified = symbol.replace(ANON, "x" * len(ANON))

        # Neutralize < and > that are part of operator token symbols — they must not
        # alter template-bracket depth. Affected operators: operator<, operator>,
        # operator<=, operator>=, operator<<, operator>>, operator<=>, operator->,
        # operator->*, operator<<=, operator>>=, and combinations thereof.
        # Word-boundary lookbehind (?<![A-Za-z_\d]) avoids matching names like
        # "custom_operator<T>" that merely end in "operator".
        import re as _re
        modified = _re.sub(
            r"(?<![A-Za-z_\d])operator([<>\-][<>=\-\*]*)",
            lambda m: "operator" + m.group(1).replace("<", "_").replace(">", "_"),
            modified,
        )

        # Step 1: scan for first '(' at <> depth 0 — that's the start of the arg list.
        # Step 2: track last ' ' at <> depth 0 before that — that separates return type
        #         from the qualified function name.
        # Only <> affect bracket depth; () are not nested here (arg list '(' is what we seek).
        depth = 0
        first_paren = len(modified)
        last_space = -1
        for i, c in enumerate(modified):
            if c == "<":
                depth += 1
            elif c == ">":
                depth -= 1
            elif depth == 0:
                if c == "(":
                    first_paren = i
                    break
                elif c == " ":
                    last_space = i

        # Handle conversion operators: "DB::Foo::operator int", "DB::Foo::operator Type const&".
        # The forward scan's last_space may land anywhere inside "operator <type>", not just
        # immediately after "operator", so checking endswith("operator") is insufficient.
        # Real examples from CIDB:
        #   AMQP::Field::operator AMQP::Array const&() const  -> last_space after "Array"
        #   AMQP::NumericField<...>::operator short() const   -> last_space inside template
        #
        # Strategy: find the last "operator " token at bracket depth 0 before first_paren,
        # with a word-boundary check (preceded by '::' / ' ' / string-start) to avoid
        # matching "custom_operator ". If found, override last_space to the depth-0 space
        # just before "operator" (i.e., separate return type from function name there).
        pre = modified[:first_paren]
        conv_op_pos = -1  # start index of "operator" if this is a conversion operator
        search_start = 0
        while True:
            pos = pre.find("operator ", search_start)
            if pos == -1:
                break
            # Word boundary: "operator" preceded by '::', ' ', or at string start
            if pos == 0 or pre[pos - 1] in (":", " "):
                # Verify this position is at bracket depth 0
                d_check = sum(
                    1 if ch == "<" else -1 if ch == ">" else 0 for ch in pre[:pos]
                )
                if d_check == 0:
                    conv_op_pos = pos  # keep the last qualifying occurrence
            search_start = pos + 1

        if conv_op_pos >= 0:
            # Override last_space: find the last depth-0 space before "operator"
            d2 = 0
            last_space = -1
            for j in range(conv_op_pos):
                c = modified[j]
                if c == "<":
                    d2 += 1
                elif c == ">":
                    d2 -= 1
                elif d2 == 0 and c == " ":
                    last_space = j

        # Everything from (last_space + 1) to first_paren is the qualified function name.
        # Use positions on the ORIGINAL symbol (same lengths after placeholder substitution).
        func_name = symbol[last_space + 1 : first_paren]

        # Step 3: strip ALL template args '<...>' from the function name.
        # The normalized symbol is used as the second arg to hasAllTokens(), which
        # tokenizes it with splitByNonAlpha and requires all tokens present in the stored
        # symbol.  Template arg tokens (type names, integer literals, etc.) are "extras"
        # that must NOT appear in the query — otherwise only the exact instantiation
        # matches and other instantiations of the same template are missed.
        # 'DB::Foo<int>::bar'        → 'DB::Foo::bar'   (class template)
        # 'DB::Foo::setUsed<true>'   → 'DB::Foo::setUsed'  (function template)
        # 'Foo<T>::bar<U>'           → 'Foo::bar'        (both)
        #
        # Uses the same two same-length preprocessing transforms so that positions map
        # 1:1 between the processed and pre-processed strings:
        #   a) (anonymous namespace) → placeholder  — neutralises its '(' and ')'
        #   b) operator<</>>/->  neutralisation    — neutralises operator < > as '_'
        # Characters are read from the pre-neutralised string so that e.g. operator<<
        # is preserved in the output with its original '<' characters.
        PLACEHOLDER = "x" * len(ANON)
        fn_pre = func_name.replace(ANON, PLACEHOLDER)
        fn_mod3 = _re.sub(
            r"(?<![A-Za-z_\d])operator([<>\-][<>=\-\*]*)",
            lambda m: "operator" + m.group(1).replace("<", "_").replace(">", "_"),
            fn_pre,
        )
        result_chars = []
        d3 = 0
        for idx, ch in enumerate(fn_mod3):
            if ch == "<":
                d3 += 1
            elif ch == ">":
                d3 -= 1
            elif d3 == 0:
                result_chars.append(fn_pre[idx])
        stripped = "".join(result_chars).replace(PLACEHOLDER, ANON)
        if stripped:
            func_name = stripped

        return func_name if func_name else symbol

    @staticmethod
    def _escape_sql_string(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")

    # Symbols covering more than this fraction of all tests are too common to be
    # useful for test selection (e.g. PipelineExecutor::execute runs in every query).
    MAX_SYMBOL_COVERAGE_PCT = 5

    # Minimum absolute test count floor — prevents over-filtering in small corpora.
    MIN_TESTS_THRESHOLD = 50

    # Absolute cap: a symbol covering more than this many tests is excluded
    # regardless of the percentage threshold. Keeps result sets manageable and
    # consistent with the Python-side max_tests_per_symbol in get_most_relevant_tests().
    MAX_TESTS_PER_SYMBOL = 200

    def get_tests_by_changed_symbols(self, symbols: list) -> dict:
        """
        Single batch query replacing the previous N+1 per-symbol round-trips.

        Uses hasAllTokens() with a splitByNonAlpha text index on the symbol column.
        This single approach handles all three matching scenarios:
          1. New normalized format (args stripped at export): tokens match exactly
          2. Old format in CIDB (full args present): token match ignores argument tokens
          3. Template instantiations (class template args differ): template arg tokens
             are extra tokens in the CIDB symbol but hasAllTokens only requires the
             query tokens to ALL be present — extra tokens are allowed

        CIDB symbols are normalized in-SQL (same stripping logic as export_coverage.py)
        so results are grouped by qualified function name and mapped back to input symbols.

        Symbols covering more than MAX_SYMBOL_COVERAGE_PCT% of all tests are excluded —
        they carry no signal for targeted test selection.
        """
        import time
        t0 = time.monotonic()

        if not symbols:
            return {sym: [] for sym in symbols}

        # Normalize each input symbol; deduplicate — multiple input symbols
        # (e.g. different overloads) can map to the same normalized form.
        norm_to_originals: dict[str, list] = {}
        for sym in symbols:
            norm = self.normalize_symbol(sym)
            norm_to_originals.setdefault(norm, []).append(sym)

        print(
            f"[find_tests] input={len(symbols)} symbols, "
            f"unique_normalized={len(norm_to_originals)}"
        )

        # Build per-symbol hasAllTokens conditions.
        # Pass the normalized symbol string directly — ClickHouse tokenizes it with
        # the same splitByNonAlpha tokenizer used for the index, so no Python-side
        # token splitting is needed. hasAllTokens(symbol, 'DB::Foo::bar') matches:
        #   - 'DB::Foo::bar'                (exact, new format)
        #   - 'DB::Foo::bar(arg1, arg2)'    (old format with args — arg tokens are extras)
        #   - 'DB::Foo<T>::bar() const'     (class template — T is an extra token)
        match_conditions = []
        for norm in norm_to_originals:
            if norm:
                match_conditions.append(
                    f"hasAllTokens(symbol, '{self._escape_sql_string(norm)}')"
                )

        if not match_conditions:
            return {sym: [] for sym in symbols}

        combined_match = " OR ".join(f"({c})" for c in match_conditions)
        print(f"[find_tests] built {len(match_conditions)} hasAllTokens conditions")

        # The SQL normalization expression — strips argument list from CIDB symbols,
        # identical to the logic in export_coverage.py.  Used to group template
        # instantiations and old-format symbols under the same key.
        SQL_NORMALIZE = (
            "if(position(replaceAll(symbol, '(anonymous namespace)', repeat('x', 21)), '(') > 0,"
            "   substring(symbol, 1, position(replaceAll(symbol, '(anonymous namespace)', repeat('x', 21)), '(') - 1),"
            "   symbol)"
        )

        query = f"""
        SELECT
            {SQL_NORMALIZE} AS norm_symbol,
            groupArray(DISTINCT test_name) AS tests
        FROM checks_coverage_inverted
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND ({combined_match})
        GROUP BY norm_symbol
        HAVING count(DISTINCT test_name) < least(
            -- Percentage-based cap: exclude symbols covering >MAX_SYMBOL_COVERAGE_PCT%
            -- of all tests (hot-path functions like PipelineExecutor::execute that
            -- run in every query carry no signal for targeted test selection).
            greatest(
                toUInt64((
                    SELECT count(DISTINCT test_name) * {self.MAX_SYMBOL_COVERAGE_PCT} / 100
                    FROM checks_coverage_inverted
                    WHERE check_start_time > now() - interval 3 days
                      AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                )),
                toUInt64({self.MIN_TESTS_THRESHOLD})
            ),
            -- Absolute cap: never return more than MAX_TESTS_PER_SYMBOL tests for
            -- a single symbol regardless of corpus size. Keeps result sets
            -- manageable and consistent with the Python-side max_tests_per_symbol
            -- filter in get_most_relevant_tests().
            toUInt64({self.MAX_TESTS_PER_SYMBOL})
        )
        """

        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
        t_query = time.monotonic()
        raw = cidb.query(query, log_level="")
        print(f"[find_tests] CIDB query: {time.monotonic()-t_query:.2f}s, response={len(raw)} bytes")

        # Parse TSV: norm_symbol \t ['test1','test2',...]
        norm_to_tests: dict[str, list] = {}
        for line in raw.strip().splitlines():
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            norm_sym, tests_raw = parts
            try:
                tests = ast.literal_eval(tests_raw.strip())
                norm_to_tests[norm_sym] = tests if isinstance(tests, list) else []
            except (ValueError, SyntaxError):
                print(f"Failed to parse tests for '{norm_sym}': {tests_raw[:100]}")

        # Map results back to original input symbols.
        # The query already unioned all matching CIDB symbols (old/new format, all
        # template instantiations) via hasAllTokens — no further expansion needed here.
        # Multiple originals sharing the same normalized form get the same test list.
        def _tokens(s: str) -> set:
            """Replicate ClickHouse splitByNonAlpha tokenization in Python."""
            return {t for t in re.split(r'[^a-zA-Z0-9_]', s) if t}

        symbol_to_tests: dict[str, list] = {}
        for norm, originals in norm_to_originals.items():
            tests: set[str] = set()
            norm_tokens = _tokens(norm)
            for result_norm, result_tests in norm_to_tests.items():
                if norm_tokens <= _tokens(result_norm):
                    tests.update(result_tests)
            tests_list = sorted(tests)
            for orig in originals:
                symbol_to_tests[orig] = tests_list

        # Fill in empty list for any symbol that got no matches
        for sym in symbols:
            symbol_to_tests.setdefault(sym, [])

        total_unique_tests = len({t for tests in symbol_to_tests.values() for t in tests})
        symbols_with_tests = sum(1 for tests in symbol_to_tests.values() if tests)
        print(
            f"[find_tests] done in {time.monotonic()-t0:.2f}s: "
            f"{symbols_with_tests}/{len(symbols)} symbols matched, "
            f"{total_unique_tests} unique tests selected"
        )
        return symbol_to_tests

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

    def get_map_file_line_to_symbol_tests(self, binary_path):
        """
        Build a mapping from (file, line) to (resolved symbol, [tests]).
        Returns:
            dict: {(file, line): (symbol or None, [tests])}
        """
        assert self.info.pr_number > 0, "Find tests by diff applicable for PRs only"
        dts = DiffToSymbols(binary_path, self.info.pr_number)
        file_line_to_address_linkagename_symbol = dts.get_map_line_to_symbol()
        not_resolved_file_lines = {}
        symbols_to_file_lines = {}

        for (file_, line_), (
            address,
            linkage_name,
            symbol,
        ) in file_line_to_address_linkagename_symbol.items():
            if symbol in symbols_to_file_lines:
                continue
            if not symbol:
                if file_ not in not_resolved_file_lines:
                    not_resolved_file_lines[file_] = set()
                if (
                    line_ - 1 in not_resolved_file_lines[file_]
                ):  # skip consecutive lines
                    continue
                not_resolved_file_lines[file_].add(line_)
            else:
                symbols_to_file_lines[symbol] = (file_, line_)

        # Fetch mapping of symbols to tests from the coverage database
        symbol_to_tests = self.get_tests_by_changed_symbols(
            list(symbols_to_file_lines.keys())
        )
        map_file_line_to_test = {}
        for symbol, tests in symbol_to_tests.items():
            map_file_line_to_test[
                (symbols_to_file_lines[symbol][0], symbols_to_file_lines[symbol][1])
            ] = (symbol, list(set(tests)))
        for file_, lines in not_resolved_file_lines.items():
            for line in lines:
                map_file_line_to_test[(file_, line)] = (None, [])

        return map_file_line_to_test

    def get_most_relevant_tests(self, binary_path, max_tests_per_symbol=100):
        """
        1. Makes a best effort to get changed symbols by reading the PR diff and the ClickHouse binary DWARF.
        2. Gets a list of tests that cover each found symbol from the coverage database.
        3. Skips symbols with more than 'max_tests_per_symbol' tests (too common code).
        4. Returns the unique tests and a Result with info about the findings.
        """

        file_line_to_symbol_tests = self.get_map_file_line_to_symbol_tests(binary_path)
        not_resolved_file_lines = {}
        resolved_file_lines = {}
        symbols_to_tests = {}
        selected_tests = set()

        for (file_, line_), (symbol, tests) in file_line_to_symbol_tests.items():
            if not tests:
                if (file_, line_) not in not_resolved_file_lines:
                    not_resolved_file_lines[(file_, line_)] = []
                not_resolved_file_lines[(file_, line_)] = symbol
            else:
                if symbol in symbols_to_tests:
                    continue
                symbols_to_tests[symbol] = tests
                resolved_file_lines[(file_, line_)] = (symbol, tests)

        info = "Tests not found for lines:\n"
        for (file_, line), symbol in not_resolved_file_lines.items():
            info += f"  {file_}:{line} -> symbol: {symbol[:70] + '...' if symbol else 'NOT FOUND'}\n"
        info = "Tests found for lines:\n"
        if not resolved_file_lines:
            info += "  No updates in source code\n"
        else:
            for (file_, line), (symbol, tests) in resolved_file_lines.items():
                info += f"  {file_}:{line} -> symbol: {symbol[:70]}...\n"
                if len(tests) > max_tests_per_symbol:
                    info += f"    skipping {len(tests)} tests (too common code)\n"
                else:
                    selected_tests.update(tests)
            for test in tests[:10]:
                info += f"  - {test}\n"
            if len(tests) > 10:
                info += f"    ... and {len(tests) - 10} more tests\n"
        info += f"Total unique tests: {len(selected_tests)}\n"
        selected_tests = list(selected_tests)
        return selected_tests, Result(
            name="tests found by coverage", status=Result.StatusExtended.OK, info=info
        )

    def get_all_relevant_tests_with_info(self, ch_path):
        tests = set()
        results = []

        # Integration tests run changed test suboptimally (entire module), it might be too long
        # limit it to stateless tests only
        if self.job_type == self.STATELESS_JOB_TYPE:
            changed_tests, result = self.get_changed_or_new_tests_with_info()
            tests.update(changed_tests)
            results.append(result)

        previously_failed_tests, result = self.get_previously_failed_tests_with_info()
        tests.update(previously_failed_tests)
        results.append(result)

        # TODO: Add coverage supoort for Integration tests
        if self.job_type == self.STATELESS_JOB_TYPE:
            try:
                covering_tests, result = self.get_most_relevant_tests(ch_path)
                tests.update(covering_tests)
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

        return tests, Result(
            name="Fetch relevant tests",
            status=Result.Status.SUCCESS,
            info=f"Found {len(tests)} relevant tests",
            results=results,
        )


if __name__ == "__main__":
    # local run tests
    parser = argparse.ArgumentParser(
        description="List changed symbols for a PR by parsing the diff and querying ClickHouse."
    )
    parser.add_argument("pr", help="Pull request number")
    parser.add_argument(
        "clickhouse_path",
        help='Path to the clickhouse binary (executed as "clickhouse local")',
    )
    args = parser.parse_args()

    class InfoLocalTest:
        pr_number = int(args.pr)
        is_local_run = True
        job_name = "Stateless"

    info = InfoLocalTest()
    targeting = Targeting(info)
    file_line_to_symbol_tests = targeting.get_map_file_line_to_symbol_tests(
        args.clickhouse_path
    )

    print("\nNo tests found for lines:")
    for (file, line), (symbol, tests) in file_line_to_symbol_tests.items():
        if tests:
            continue
        print(
            f"{file}:{line} -> symbol [{symbol[:70] + '...' if symbol else 'NOT FOUND'}"
        )

    print("\nTests found for lines:")
    for (file, line), (symbol, tests) in file_line_to_symbol_tests.items():
        if not tests:
            continue
        print(f"{file}:{line} -> symbol [{symbol[:70]}...]:")
        for test in tests[:10]:
            print(f" - {test}")
        if len(tests) > 10:
            print(f" - ... and {len(tests) - 10} more tests")
