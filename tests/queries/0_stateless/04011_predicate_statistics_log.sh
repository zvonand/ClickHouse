#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="test_pred_ext_${CLICKHOUSE_DATABASE}"

ENABLE_STATS="SET predicate_statistics_sample_rate = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"
ENABLE_STATS_SINGLE_STEP="$ENABLE_STATS, enable_multiple_prewhere_read_steps = 0"

$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE (
    id UInt64,
    status String,
    category String,
    score Float64,
    tag Nullable(String)
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO $TABLE SELECT
    number,
    if(number % 10 = 0, 'active', 'inactive'),
    if(number % 3 = 0, 'cat_a', 'cat_b'),
    number * 0.1,
    if(number % 5 = 0, NULL, 'val')
FROM numbers(100000);
"

# ---- Test 1: All predicate classes ----

$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS; SELECT * FROM $TABLE WHERE status = 'active' FORMAT Null;
$ENABLE_STATS; SELECT * FROM $TABLE WHERE score > 5000.0 FORMAT Null;
$ENABLE_STATS; SELECT * FROM $TABLE WHERE id IN (1, 2, 3, 4, 5) FORMAT Null;
$ENABLE_STATS; SELECT * FROM $TABLE WHERE status LIKE '%act%' FORMAT Null;
$ENABLE_STATS; SELECT * FROM $TABLE WHERE tag IS NULL FORMAT Null;
SYSTEM FLUSH LOGS predicate_statistics_log;
"

echo '--- predicate classes ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    column_name,
    predicate_class,
    function_name,
    sum(input_rows) > 0 AS has_input,
    sum(passed_rows) > 0 AS has_passed
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name != ''
GROUP BY column_name, predicate_class, function_name
ORDER BY column_name, predicate_class, function_name;
"

# ---- Test 2: 100% selectivity (all rows pass) ----

$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS; SELECT * FROM $TABLE WHERE id >= 0 FORMAT Null;
SYSTEM FLUSH LOGS predicate_statistics_log;
"

echo '--- 100% selectivity ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    round(max(filter_selectivity), 1) AS sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name = 'id' AND function_name = 'greaterOrEquals';
"

# ---- Test 3: 0% selectivity (no rows pass) ----

$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS; SELECT * FROM $TABLE WHERE status = 'nonexistent' FORMAT Null;
SYSTEM FLUSH LOGS predicate_statistics_log;
"

echo '--- 0% selectivity ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    sum(input_rows) > 0 AS has_input,
    sum(passed_rows) = 0 AS zero_passed
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name = 'status' AND function_name = 'equals'
    AND passed_rows = 0;
"

# ---- Test 4: Multi-column predicate (a > b) should produce no log entries ----

TABLE_MC="${TABLE}_mc"
$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS;
DROP TABLE IF EXISTS $TABLE_MC;
CREATE TABLE $TABLE_MC (id UInt64, score Float64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO $TABLE_MC SELECT number, number * 0.5 FROM numbers(1000);
$ENABLE_STATS; SELECT * FROM $TABLE_MC WHERE id > score FORMAT Null;
SYSTEM FLUSH LOGS predicate_statistics_log;
"

echo '--- multi-column skipped ---'
$CLICKHOUSE_CLIENT --query "SELECT count() = 0 AS skipped FROM system.predicate_statistics_log WHERE table = '$TABLE_MC' AND column_name != ''"

# ---- Test 5: predicate_statistics_sample_rate = 0 → nothing logged ----

TABLE2="${TABLE}_disabled"
$CLICKHOUSE_CLIENT -m --query "
SET predicate_statistics_sample_rate = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
DROP TABLE IF EXISTS $TABLE2;
CREATE TABLE $TABLE2 (id UInt64, status String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO $TABLE2 SELECT number, 'x' FROM numbers(1000);
SELECT * FROM $TABLE2 WHERE status = 'x' FORMAT Null;
SYSTEM FLUSH LOGS predicate_statistics_log;
"

echo '--- disabled ---'
$CLICKHOUSE_CLIENT --query "SELECT count() = 0 AS nothing_logged FROM system.predicate_statistics_log WHERE table = '$TABLE2'"

# ---- Test 6: filter_expression is not empty ----

echo '--- filter_expression ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    length(filter_expression) > 0 AS has_expr
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name != ''
LIMIT 1;
"

# ---- Test 7: total_selectivity in conjunction ----

$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS_SINGLE_STEP; SELECT * FROM $TABLE WHERE status = 'active' AND category = 'cat_a' FORMAT Null;
SYSTEM FLUSH LOGS predicate_statistics_log;
"

echo '--- conjunction total_selectivity ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    count() >= 2 AS has_atoms,
    round(min(total_selectivity), 2) = round(max(total_selectivity), 2) AS same_whole_sel,
    min(total_selectivity) < 0.1 AS selective
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name != ''
    AND query_id IN (
        SELECT query_id FROM system.predicate_statistics_log
        WHERE table = '$TABLE' AND column_name != ''
        GROUP BY query_id HAVING countDistinct(column_name) >= 2
    );
"

# ---- Test 8: Selectivity values are sane (between 0 and 1) ----

echo '--- selectivity bounds ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    min(filter_selectivity) >= 0 AS min_ok,
    max(filter_selectivity) <= 1 AS max_ok,
    min(total_selectivity) >= 0 AS total_min_ok,
    max(total_selectivity) <= 1 AS total_max_ok
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND column_name != '';
"

# ---- Test 9: passed_rows <= input_rows always ----

echo '--- passed <= input ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT count() = 0 AS ok
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND passed_rows > input_rows;
"

# ---- Cleanup ----
$CLICKHOUSE_CLIENT --query "DROP TABLE $TABLE"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE2"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_MC"
