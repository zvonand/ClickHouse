#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-debug

# Test lazy FINAL optimization with all three filtering mechanisms:
# 1. Non-intersecting parts split (~20% of total marks)
# 2. PK index analysis via set (~25% of intersecting marks pruned by key range)
# 3. Non-PK predicate (status = 'active' filters ~50% of remaining rows)
#
# Data layout (ORDER BY key, index_granularity=128):
#   Parts 1,2: keys 0..3999 (overlapping → intersecting), ~32 marks each
#   Part 3: keys 4000..4999 (non-intersecting), ~8 marks
#   Part 4: keys 5000..5999 (non-intersecting), ~8 marks
#
# WHERE key >= 1000 AND key < 5500 AND status = 'active':
#   key range → PK prunes keys 0..999 and 5500..5999 (~25% of intersecting marks)
#   Non-intersecting parts partially survive (keys 4000..5499)
#   status='active' for even keys → filters ~50% of rows in PK-selected range

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_combined;
    CREATE TABLE t_lazy_final_combined
    (
        key UInt64,
        version UInt64,
        status String,
        value UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY key
    SETTINGS index_granularity = 128;

    SYSTEM STOP MERGES t_lazy_final_combined;

    -- Intersecting pair: keys 0..3999 in two parts with different versions.
    INSERT INTO t_lazy_final_combined SELECT
        number, 1,
        if(number % 2 = 0, 'active', 'inactive'),
        number
    FROM numbers(4000);

    INSERT INTO t_lazy_final_combined SELECT
        number, 2,
        if(number % 2 = 0, 'active', 'inactive'),
        number * 10
    FROM numbers(4000);

    -- Non-intersecting parts: keys 4000..4999 and 5000..5999.
    INSERT INTO t_lazy_final_combined SELECT
        number + 4000, 1,
        if(number % 2 = 0, 'active', 'inactive'),
        number + 4000
    FROM numbers(1000);

    INSERT INTO t_lazy_final_combined SELECT
        number + 5000, 1,
        if(number % 2 = 0, 'active', 'inactive'),
        number + 5000
    FROM numbers(1000);
"

## Test 1: Correctness — results must match with and without optimization.
echo "=== Correctness ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

## Test 2: Plan has both Union (non-intersecting split) and InputSelector.
echo "=== Plan structure ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'Union'

$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'InputSelector'

## Test 3: Verify PK pruning — LazyFinalKeyAnalysisTransform should show marks filtered.
echo "=== PK pruning ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" --send_logs_level='trace' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Lazy FINAL enabled' \
    | sed 's/.*total_marks=/total_marks=/' \
    | sed -E 's/set_rows=[0-9]+/set_rows=N/' \
    | head -1

## Test 4: Verify the "Selected" line shows reduced parts/marks after PK index.
echo "=== Selected after index ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

## Test 5: Fallback path still correct.
echo "=== Fallback (set truncated) ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 1000 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10
"

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_combined"
