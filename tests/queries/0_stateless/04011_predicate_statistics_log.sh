#!/usr/bin/env bash
# Tags: no-fasttest
# Should use server because table must be created with config

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="test_pred_stats_${CLICKHOUSE_DATABASE}"

SETTINGS="SET predicate_statistics_sample_rate = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

$CLICKHOUSE_CLIENT -m --query "
$SETTINGS;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE (id UInt64, status String, value Float64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO $TABLE SELECT number, if(number % 10 = 0, 'active', 'inactive'), rand() FROM numbers(100000);

SELECT * FROM $TABLE WHERE status = 'active' FORMAT Null;
SELECT * FROM $TABLE WHERE id > 50000 FORMAT Null;
SELECT * FROM $TABLE WHERE status = 'active' AND id > 50000 FORMAT Null SETTINGS enable_multiple_prewhere_read_steps = 0;

SYSTEM FLUSH LOGS predicate_statistics_log;
"

# q1: ~10% selectivity
$CLICKHOUSE_CLIENT -m --query "
SELECT
    column_name = 'status' AS col_ok,
    predicate_class = 'Equality' AS class_ok,
    input_rows > 0 AS has_input,
    passed_rows > 0 AS has_passed,
    round(filter_selectivity, 1) AS sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name = 'status'
    AND function_name = 'equals'
LIMIT 1;
"

# q2: PK prunes, remaining mostly pass
$CLICKHOUSE_CLIENT -m --query "
SELECT
    column_name = 'id' AS col_ok,
    predicate_class = 'Range' AS class_ok,
    input_rows > 0 AS has_input,
    filter_selectivity > 0.9 AS high_sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name = 'id'
    AND function_name = 'greater'
LIMIT 1;
"

# q3: conjunction — filter to rows where both atoms are present (same query_id)
$CLICKHOUSE_CLIENT -m --query "
SELECT
    count() >= 2 AS has_atoms,
    min(total_selectivity) < 0.15 AS selective,
    max(total_input_rows) > 0 AS has_input,
    max(total_passed_rows) > 0 AS has_passed,
    min(total_selectivity) = max(total_selectivity) AS same_whole_sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name != ''
    AND query_id IN (
        SELECT query_id FROM system.predicate_statistics_log
        WHERE table = '$TABLE' AND column_name != ''
        GROUP BY query_id HAVING count() >= 2
    );
"

$CLICKHOUSE_CLIENT --query "DROP TABLE $TABLE"
