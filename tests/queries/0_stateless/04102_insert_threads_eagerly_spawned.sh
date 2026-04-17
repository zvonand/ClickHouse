#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: checks thread count, which can be affected by concurrent queries

# Verifies that a plain INSERT (no SELECT, no MVs) does not request
# excessive ConcurrencyControl slots or spawn unnecessary threads.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/102947

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_insert_threads"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_insert_threads_mv"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_insert_threads (x UInt64) ENGINE = MergeTree ORDER BY x"

# Test 1: Plain INSERT FORMAT TSV with max_threads=16, no MVs.
# The insert pipeline is a single chain — should use max_insert_threads (1).
echo "=== Plain INSERT without MVs ==="
QUERY_ID1="04102_no_mv_$RANDOM"

$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(10000) FORMAT TSV" | \
$CLICKHOUSE_CLIENT \
    --query_id="$QUERY_ID1" \
    --max_threads=16 \
    --max_insert_threads=1 \
    --log_queries=1 \
    --send_logs_level=trace \
    -q "INSERT INTO test_insert_threads FORMAT TSV" 2>"${CLICKHOUSE_TMP}/04102_no_mv.txt"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT -q "
    SELECT
        if(peak_threads_usage <= 4, 'FEW THREADS', 'MANY THREADS SPAWNED')
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$QUERY_ID1'
"

# Verify CC allocation uses max_insert_threads (1), not max_threads (16)
grep -c 'Allocating CPU slots from ConcurrencyControl: min=1, max=1' "${CLICKHOUSE_TMP}/04102_no_mv.txt"


# Test 2: INSERT with a materialized view — should use max_threads.
echo "=== Plain INSERT with MV ==="
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW test_insert_threads_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM test_insert_threads"

QUERY_ID2="04102_with_mv_$RANDOM"

$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(10000) FORMAT TSV" | \
$CLICKHOUSE_CLIENT \
    --query_id="$QUERY_ID2" \
    --max_threads=16 \
    --max_insert_threads=1 \
    --log_queries=1 \
    --send_logs_level=trace \
    -q "INSERT INTO test_insert_threads FORMAT TSV" 2>"${CLICKHOUSE_TMP}/04102_with_mv.txt"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

# With MVs, should request max_threads (16) for MV inner selects
grep -c 'Allocating CPU slots from ConcurrencyControl: min=1, max=16' "${CLICKHOUSE_TMP}/04102_with_mv.txt"


$CLICKHOUSE_CLIENT -q "DROP TABLE test_insert_threads_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_insert_threads"
