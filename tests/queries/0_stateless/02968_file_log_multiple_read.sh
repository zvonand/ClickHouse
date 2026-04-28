#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

rm -rf ${logs_dir}

mkdir -p ${logs_dir}/

for i in {1..10}
do
	echo $i >> ${logs_dir}/a.txt
done

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS file_log;
DROP TABLE IF EXISTS table_to_store_data;
DROP TABLE IF EXISTS file_log_mv;

CREATE TABLE file_log (
    id Int64
) ENGINE = FileLog('${logs_dir}/', 'CSV')
SETTINGS poll_directory_watch_events_backoff_max = 1000;

CREATE TABLE table_to_store_data (
    id Int64
) ENGINE = MergeTree
ORDER BY id;

CREATE MATERIALIZED VIEW file_log_mv TO table_to_store_data AS
    SELECT id
    FROM file_log
    WHERE id NOT IN (
        SELECT id
        FROM table_to_store_data
        WHERE id IN (
            SELECT id
            FROM file_log
        )
    );
" || exit 1

function count()
{
	COUNT=$(${CLICKHOUSE_CLIENT} --query "select count() from table_to_store_data;")
	echo $COUNT
}

function wait_for_row_count()
{
    local threshold="$1"
    # `FileLog` polls the directory using exponential backoff. The default
    # `poll_directory_watch_events_backoff_max` is 32s, which combined with
    # `BackgroundSchedulePool` contention under sanitizer / heavy CI load can
    # push file-detection latency well beyond a tight 30s test timeout.
    # The table above caps `poll_directory_watch_events_backoff_max` at 1s so
    # worst-case detection is bounded at ~1s + scheduling delay; the 120s
    # timeout below is a generous safety net (matching the 02889 pattern).
    local timeout=120
    local start=$EPOCHSECONDS
    while [[ $(count) -lt threshold ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout (${timeout}s) waiting for the minimum number of rows, expected at least ${threshold} row(s). Got $(count)."
            exit 1
        fi
        sleep 0.5
    done
}

wait_for_row_count 1

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table_to_store_data ORDER BY id;"

for i in {1..20}
do
	echo $i >> ${logs_dir}/a.txt
done

wait_for_row_count 11

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table_to_store_data ORDER BY id;"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log;
DROP TABLE table_to_store_data;
DROP TABLE file_log_mv;
"

rm -rf ${logs_dir}
