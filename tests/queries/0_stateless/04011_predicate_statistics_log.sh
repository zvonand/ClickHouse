#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

q1="pred_q1_${CLICKHOUSE_DATABASE}_$$"
q2="pred_q2_${CLICKHOUSE_DATABASE}_$$"
q3="pred_q3_${CLICKHOUSE_DATABASE}_$$"

$CLICKHOUSE_CLIENT -m --query "
SET predicate_statistics_sample_rate = 1;

DROP TABLE IF EXISTS test_pred;
CREATE TABLE test_pred (id UInt64, status String, value Float64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO test_pred SELECT number, if(number % 10 = 0, 'active', 'inactive'), rand() FROM numbers(100000);
"

# SELECT * to ensure PREWHERE is used

# ~10% selectivity
$CLICKHOUSE_CLIENT --query_id="$q1" --query \
    "SET predicate_statistics_sample_rate = 1; SELECT * FROM test_pred WHERE status = 'active' FORMAT Null"

# ~50% after PK prune
$CLICKHOUSE_CLIENT --query_id="$q2" --query \
    "SET predicate_statistics_sample_rate = 1; SELECT * FROM test_pred WHERE id > 50000 FORMAT Null"

# conjunction ~5%
$CLICKHOUSE_CLIENT --query_id="$q3" --query \
    "SET predicate_statistics_sample_rate = 1; SELECT * FROM test_pred WHERE status = 'active' AND id > 50000 FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS predicate_statistics_log"

# q1: status = 'active' → ~10% selectivity
$CLICKHOUSE_CLIENT -m --query "
SELECT
    column_name = 'status' AS col_ok,
    predicate_class = 'Equality' AS class_ok,
    input_rows > 0 AS has_input,
    passed_rows > 0 AS has_passed,
    round(filter_selectivity, 1) AS sel
FROM system.predicate_statistics_log
WHERE query_id = '$q1' AND column_name != ''
LIMIT 1;
"

# q2: id > 50000 → PK prunes, remaining rows mostly pass (~98%)
$CLICKHOUSE_CLIENT -m --query "
SELECT
    column_name = 'id' AS col_ok,
    predicate_class = 'Range' AS class_ok,
    input_rows > 0 AS has_input,
    filter_selectivity > 0.9 AS high_sel
FROM system.predicate_statistics_log
WHERE query_id = '$q2' AND column_name != ''
LIMIT 1;
"

# q3: conjunction → total_selectivity < each step
$CLICKHOUSE_CLIENT -m --query "
SELECT
    count() >= 1 AS has_atoms,
    min(total_selectivity) < 0.15 AS whole_pred_selective,
    max(total_input_rows) > 0 AS has_total_input,
    max(total_passed_rows) > 0 AS has_total_passed,
    min(total_selectivity) = max(total_selectivity) AS same_whole_sel
FROM system.predicate_statistics_log
WHERE query_id = '$q3' AND column_name != '';
"

$CLICKHOUSE_CLIENT --query "DROP TABLE test_pred"
