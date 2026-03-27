#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Verify that system.predicate_statistics_log collects per-predicate selectivity data
# The global predicate_statistics_sample_rate is 0 to avoid overhead in unrelated tests
# this test enables it temporarily via a config override + SYSTEM RELOAD CONFIG

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_override="${CLICKHOUSE_CONFIG_DIR}/config.d/zzz_predicate_statistics_test_override.xml"

# enable predicate statistics collection for this test
cat > "$config_override" <<'EOF'
<clickhouse>
    <predicate_statistics_sample_rate>1</predicate_statistics_sample_rate>
</clickhouse>
EOF

$CLICKHOUSE_CLIENT --send_logs_level=fatal --query "SYSTEM RELOAD CONFIG"

# on exit: disable the override and reload config
trap 'rm -f "$config_override"; $CLICKHOUSE_CLIENT --send_logs_level=fatal --query "SYSTEM RELOAD CONFIG" 2>/dev/null' EXIT

index_qid="pred_stats_index_${CLICKHOUSE_DATABASE}_$$"
filter_qid="pred_stats_filter_${CLICKHOUSE_DATABASE}_$$"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test_pred_stats;

CREATE TABLE test_pred_stats (id UInt64, status String, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_pred_stats SELECT number, if(number % 10 = 0, 'active', 'inactive'), rand() FROM numbers(100000);
"

# Direct MergeTree query triggers index-level logging
$CLICKHOUSE_CLIENT --query_id="$index_qid" --query "SELECT count() FROM test_pred_stats WHERE id > 50000 FORMAT Null"

# numbers() always produces a FilterTransform (MergeTree may push filter into reader)
$CLICKHOUSE_CLIENT --query_id="$filter_qid" --query "SELECT count() FROM numbers(100000) WHERE number > 50000 FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS predicate_statistics_log"

# Verify index-level entry tied to this run's query_id
$CLICKHOUSE_CLIENT -m --query "
SELECT
    filter_expression != '' AS has_filter_expr,
    length(index_names) > 0 AS has_index_names,
    length(index_names) = length(index_types) AS names_types_match,
    length(index_names) = length(total_granules) AS names_granules_match,
    length(index_names) = length(index_selectivities) AS names_sel_match,
    arrayAll(x -> x >= 0 AND x <= 1, index_selectivities) AS valid_selectivities,
    arrayAll((t, a) -> t >= a, total_granules, granules_after) AS granules_consistent
FROM system.predicate_statistics_log
WHERE query_id = '$index_qid'
    AND length(index_names) > 0
LIMIT 1;

-- Verify filter-level entry tied to this run's query_id
SELECT
    column_name = 'number' AS correct_column,
    predicate_class = 'Range' AS correct_class,
    function_name = 'greater' AS correct_function,
    input_rows > 0 AS has_input,
    filter_selectivity >= 0 AND filter_selectivity <= 1 AS valid_selectivity
FROM system.predicate_statistics_log
WHERE query_id = '$filter_qid'
    AND column_name != ''
LIMIT 1;

DROP TABLE test_pred_stats;
"
