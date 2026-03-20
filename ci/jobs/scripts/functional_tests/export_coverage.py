from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
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
        table = "coverage_log"
        path_arg = f" --path {self.src.run_path0}"

        stats_query = (
            f"SELECT count() AS rows, countIf(notEmpty(test_name)) AS rows_with_test, "
            f"uniqExact(test_name) AS tests, uniqExact(arrayJoin(files)) AS files, "
            f"round(avg(arrayAvg(arrayMap((d)->toUInt32(d), min_depths)))) AS avg_min_depth "
            f"FROM system.{table} FINAL"
        )
        stats_cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{stats_query}" {command_args_post}'
        rc_stats, stdout_stats, stderr_stats = Shell.get_res_stdout_stderr(stats_cmd, verbose=True)
        if rc_stats != 0:
            raise RuntimeError(
                f"Failed to read system.{table} statistics, stderr: {stderr_stats}"
            )
        else:
            print(f"Coverage log statistics: {stdout_stats}")
            rows = int(stdout_stats.strip().split("\t")[0])
            if rows == 0:
                raise RuntimeError(
                    f"system.{table} is empty — per-test coverage collection is broken. "
                    "Check that the server was built with WITH_COVERAGE=ON and that "
                    "SYSTEM SET COVERAGE TEST flushed data correctly."
                )

        if not self.to_file:
            query = (
                f"INSERT INTO FUNCTION remoteSecure('{self.dest.url.removeprefix('https://')}', 'default.checks_coverage_lines', '{self.dest.user}', '{self.dest.pwd}') "
                "SELECT file, line_start, line_end, "
                f"'{self.check_start_time}' AS check_start_time, "
                f"'{self.job_name}' AS check_name, "
                "test_name, "
                "min(min_depth) AS min_depth, "
                "any(branch_flag) AS branch_flag "
                f"FROM system.{table} FINAL "
                "ARRAY JOIN files AS file, line_starts AS line_start, line_ends AS line_end, "
                "min_depths AS min_depth, branch_flags AS branch_flag "
                "WHERE notEmpty(test_name) AND notEmpty(file) "
                "GROUP BY file, line_start, line_end, test_name"
            )
            cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}'
            rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=True)
            if stdout:
                print(f"Export stdout: {stdout}")
            if stderr:
                print(f"Export stderr: {stderr}")
            if rc != 0:
                raise RuntimeError(f"Failed to export coverage table: {table}")
        else:
            query = (
                "SELECT "
                "time, "
                "test_name, "
                "file, "
                "line_start, "
                "line_end, "
                "min_depth "
                f"FROM system.{table} FINAL "
                "ARRAY JOIN files AS file, line_starts AS line_start, "
                "line_ends AS line_end, min_depths AS min_depth "
                f"INTO OUTFILE '{temp_dir}/system_tables/{table}.tsv' "
                "FORMAT TSVWithNamesAndTypes"
            )
            cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}'
            rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=True)
            if rc != 0:
                raise RuntimeError(f"Failed to export coverage table to file: {table}")
