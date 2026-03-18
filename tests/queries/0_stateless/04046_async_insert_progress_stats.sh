#!/usr/bin/env bash
# Tags: no-fasttest
# Test that async insert progress stats are properly reported
# to clients and recorded in query_log for both TCP and HTTP protocols.
# https://github.com/ClickHouse/ClickHouse/issues/99758

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_insert_progress"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_async_insert_progress (x String) ENGINE = MergeTree ORDER BY x"

# Test 1: TCP protocol (clickhouse-client uses TCP)
${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --query \
    "INSERT INTO test_async_insert_progress VALUES ('one'), ('two'), ('three')"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'TCP written_rows', written_rows
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'INSERT INTO test_async_insert_progress%'
        AND type = 'QueryFinish'
        AND interface = 'TCP'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'TCP result_rows', result_rows
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'INSERT INTO test_async_insert_progress%'
        AND type = 'QueryFinish'
        AND interface = 'TCP'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

# Test 2: HTTP protocol
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1" \
    -d "INSERT INTO test_async_insert_progress VALUES ('four'), ('five'), ('six')"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'HTTP written_rows', written_rows
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'INSERT INTO test_async_insert_progress%'
        AND type = 'QueryFinish'
        AND interface = 'HTTP'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'HTTP result_rows', result_rows
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'INSERT INTO test_async_insert_progress%'
        AND type = 'QueryFinish'
        AND interface = 'HTTP'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

# Verify data actually got written
${CLICKHOUSE_CLIENT} --query "SELECT 'total_rows', count() FROM test_async_insert_progress"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_async_insert_progress"
