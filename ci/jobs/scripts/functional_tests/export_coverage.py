import os
from concurrent.futures import ThreadPoolExecutor

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.find_tests import Targeting
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


class CoverageExporter:
    LOGS_SAVER_CLIENT_OPTIONS = "--max_memory_usage 10G --max_threads 1 --max_result_rows 0 --max_result_bytes 0 --max_bytes_to_read 0 --max_execution_time 0 --max_execution_time_leaf 0 --max_estimated_execution_time 0"

    def __init__(
        self,
        src: ClickHouseProc,
        dest: CIDBCluster,
        job_name: str,
        check_start_time="",
        to_file=False,
    ):
        self.src = src
        self.dest = dest
        assert to_file or self.dest.is_ready(), "Destination cluster is not ready"
        self.job_name = job_name
        self.check_start_time = check_start_time or Utils.timestamp_to_str(
            Utils.timestamp()
        )
        self.to_file = to_file

    def do(self):
        command_args = self.LOGS_SAVER_CLIENT_OPTIONS
        # command_args += f" --config-file={self.ch_config_dir}/config.xml"
        command_args += " --only-system-tables --stacktrace"
        # we need disk definitions for S3 configurations, but it is OK to always use server config

        command_args += " --config-file=/etc/clickhouse-server/config.xml"
        # Change log files for local in config.xml as command args do not override
        Shell.check(
            f"sed -i 's|<log>.*</log>|<log>{self.src.CH_LOCAL_LOG}</log>|' /etc/clickhouse-server/config.xml"
        )
        Shell.check(
            f"sed -i 's|<errorlog>.*</errorlog>|<errorlog>{self.src.CH_LOCAL_ERR_LOG}</errorlog>|' /etc/clickhouse-server/config.xml"
        )
        # FIXME: Hack for s3_with_keeper (note, that we don't need the disk,
        # the problem is that whenever we need disks all disks will be
        # initialized [1])
        #
        #   [1]: https://github.com/ClickHouse/ClickHouse/issues/77320
        #
        #   [2]: https://github.com/ClickHouse/ClickHouse/issues/77320
        #
        command_args_post = f"-- --zookeeper.implementation=testkeeper"

        Shell.check(
            f"rm -rf {temp_dir}/system_tables && mkdir -p {temp_dir}/system_tables"
        )
        res = True

        for table in ["coverage_log"]:
            path_arg = f" --path {self.src.run_path0}"

            if not self.to_file:
                # --- Insert 1: raw symbols → checks_coverage_inverted ---
                # No SQL-side normalization: the text index (splitByNonAlpha +
                # hasAllTokens) at query time is robust to extra return-type and
                # template-arg tokens.  SQL normalization was actively harmful: its
                # naive find('(') hit cast expressions like (char8_t)15 inside
                # template args, and its first-'<' stripping over-truncated
                # class-template+function-template symbols, causing hasAllTokens to
                # miss ~27% of stored symbols.
                query = (
                    f"INSERT INTO FUNCTION remoteSecure('{self.dest.url.removeprefix('https://')}', 'default.checks_coverage_inverted', '{self.dest.user}', '{self.dest.pwd}') "
                    "SELECT DISTINCT sym AS symbol, "
                    f"'{self.check_start_time}' AS check_start_time, "
                    f"'{self.job_name}' AS check_name, "
                    "test_name "
                    f"FROM system.{table} "
                    "ARRAY JOIN symbol AS sym "
                    # Exclude __client rows: they inflate count(distinct test_name)
                    # and break the frequency filter; they also return non-runnable
                    # identifiers unsuitable for targeted test selection.
                    "WHERE notEmpty(sym) AND NOT endsWith(test_name, '__client')"
                )
                cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}'
                rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=False)
                res = rc == 0
                if not res:
                    print(f"ERROR: raw insert (checks_coverage_inverted) failed (rc={rc})")
                    if stdout:
                        print(f"  stdout: {stdout}")
                    if stderr:
                        print(f"  stderr: {stderr}")

                if res:
                    # --- Insert 2: normalized symbols → checks_coverage_inverted2 ---
                    # normalize_symbol strips return type, arg list, and ALL template
                    # args — the stored symbol is the bare qualified function name,
                    # e.g. "DB::Foo::bar".  The bloom_filter index on that table
                    # supports "WHERE symbol = 'DB::Foo::bar'" exact-match queries.

                    # Step 1: dump raw (sym, test_name) pairs to a temp file.
                    raw_file = f"{temp_dir}/coverage_raw_symbols.tsv"
                    Shell.check(f"rm -f {raw_file}")
                    select_query = (
                        f"SELECT sym, test_name "
                        f"FROM system.{table} "
                        "ARRAY JOIN symbol AS sym "
                        "WHERE notEmpty(sym) AND NOT endsWith(test_name, '__client') "
                        f"INTO OUTFILE '{raw_file}' FORMAT TSV"
                    )
                    cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{select_query}" {command_args_post}'
                    rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=False)
                    if rc != 0:
                        print(f"ERROR: normalized select failed (rc={rc})")
                        if stdout:
                            print(f"  stdout: {stdout}")
                        if stderr:
                            print(f"  stderr: {stderr}")
                        res = False

                if res:
                    # Step 2: normalize symbols in parallel, deduplicate, write TSV.
                    # normalize_symbol is stateless so chunks are independent.
                    with open(raw_file) as fin:
                        raw_pairs = [
                            tuple(line.rstrip("\n").split("\t", 1))
                            for line in fin
                            if "\t" in line
                        ]

                    workers = max(1, os.cpu_count() or 4)
                    chunk_size = max(1, len(raw_pairs) // workers)
                    chunks = [
                        raw_pairs[i : i + chunk_size]
                        for i in range(0, len(raw_pairs), chunk_size)
                    ]

                    def normalize_chunk(chunk):
                        result = []
                        for sym, test_name in chunk:
                            norm = Targeting.normalize_symbol(sym)
                            if norm:
                                result.append((norm, test_name))
                        return result

                    norm_file = f"{temp_dir}/coverage_normalized_symbols.tsv"
                    seen = set()
                    with open(norm_file, "w") as fout:
                        with ThreadPoolExecutor(max_workers=workers) as executor:
                            for normalized_chunk in executor.map(normalize_chunk, chunks):
                                for norm, test_name in normalized_chunk:
                                    key = (norm, test_name)
                                    if key not in seen:
                                        seen.add(key)
                                        fout.write(
                                            f"{norm}\t{self.check_start_time}\t{self.job_name}\t{test_name}\n"
                                        )

                    # Step 3: insert the normalized TSV into checks_coverage_inverted2.
                    insert_query = (
                        f"INSERT INTO FUNCTION remoteSecure('{self.dest.url.removeprefix('https://')}', 'default.checks_coverage_inverted2', '{self.dest.user}', '{self.dest.pwd}') "
                        "(symbol, check_start_time, check_name, test_name) FORMAT TSV"
                    )
                    cmd = (
                        f"cd {self.src.run_path0} && "
                        f"clickhouse local {command_args} {path_arg} "
                        f'--query "{insert_query}" < {norm_file} '
                        f"{command_args_post}"
                    )
                    rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=False)
                    res = rc == 0
                    if not res:
                        print(f"ERROR: normalized insert (checks_coverage_inverted2) failed (rc={rc})")
                        if stdout:
                            print(f"  stdout: {stdout}")
                        if stderr:
                            print(f"  stderr: {stderr}")
            else:
                query = (
                    "SELECT "
                    "time, "
                    "arrayJoin(symbol) AS symbol, "
                    "test_name "
                    f"FROM system.{table} "
                    f"INTO OUTFILE '{temp_dir}/system_tables/{table}.tsv' "
                    "FORMAT TSVWithNamesAndTypes"
                )
                cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}'
                rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=True)
                res = rc == 0

            if not res:
                print(f"ERROR: Failed to export coverage table: {table}")
                break
        return res
