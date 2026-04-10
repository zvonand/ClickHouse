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
INSERT INTO $TABLE SELECT number, if(number % 10 = 0, 'active', 'inactive'), number * 0.1 FROM numbers(100000);

$SETTINGS; SELECT * FROM $TABLE WHERE status = 'active' FORMAT Null;
SELECT * FROM $TABLE WHERE value > 5000.0 FORMAT Null;
SELECT * FROM $TABLE WHERE status = 'active' AND value > 5000.0 FORMAT Null SETTINGS enable_multiple_prewhere_read_steps = 0;

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
WHERE table = '$TABLE' AND column_name = 'status' AND function_name = 'equals'
ORDER BY filter_selectivity DESC
LIMIT 1;
"

# q2: value > 5000 → ~50% selectivity (non-PK, stable)
$CLICKHOUSE_CLIENT -m --query "
SELECT
    column_name = 'value' AS col_ok,
    predicate_class = 'Range' AS class_ok,
    input_rows > 0 AS has_input,
    round(filter_selectivity, 1) AS sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name = 'value' AND function_name = 'greater'
ORDER BY filter_selectivity DESC
LIMIT 1;
"

# q3: conjunction — both atoms present, total_selectivity same for all
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
