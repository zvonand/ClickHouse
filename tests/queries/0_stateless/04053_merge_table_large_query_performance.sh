#!/usr/bin/env bash
# Tags: long
# Test for https://github.com/ClickHouse/ClickHouse/issues/32465
# Enormously large query is slow when run from a Merge table with many underlying tables.
# The query tree gets cloned for each underlying table, so planning time is O(N * query_complexity).
# This test verifies the query completes within a reasonable time (under 10 seconds).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NUM_TABLES=100

# Create underlying tables
for i in $(seq 0 $((NUM_TABLES - 1))); do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_merge_perf_${i} (date Date, category String, value Int64, customer_id String) ENGINE = MergeTree ORDER BY (date, category)"
done

# Create Merge table
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_merge_perf_all (date Date, category String, value Int64, customer_id String) ENGINE = Merge('${CLICKHOUSE_DATABASE}', '^t_merge_perf_\\\\d+\$')"

# Insert a small amount of data into one table (we care about planning time, not data processing)
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}.t_merge_perf_0 SELECT
    toDate('2021-10-04') + number % 70,
    ['auto', 'appliances', 'garden', 'children', 'home', 'hobbies', 'electronics', 'books'][number % 8 + 1],
    number,
    toString(number % 1000)
FROM numbers(1000)"

# Build a large multiIf expression block (simulating the original issue with ~16 categories)
CATEGORIES=("auto" "appliances" "garden" "children" "home" "hobbies" "electronics" "books"
            "computers" "beauty" "equipment" "clothing" "food" "sports" "construction" "health")

build_multiif() {
    local field=$1
    local result="multiIf("
    for cat in "${CATEGORIES[@]}"; do
        result+="position(${field}, '${cat}') > 0, '${cat}_mapped', "
    done
    result+="'other')"
    echo "$result"
}

MULTIIF_CAT=$(build_multiif "category")

# Build the large query with repeated multiIf expressions in both SELECT and WHERE
# This simulates the original ~1260-line machine-generated query
QUERY="SELECT
    count(),
    sum(value),
    uniqExact(customer_id),
    uniqExact(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 0, customer_id,
        ${MULTIIF_CAT} >= 'b' AND value > 1, customer_id,
        ${MULTIIF_CAT} >= 'c' AND value > 2, customer_id,
        ${MULTIIF_CAT} >= 'd' AND value > 3, customer_id,
        ''
    )),
    uniqExact(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 10, customer_id,
        ${MULTIIF_CAT} >= 'b' AND value > 11, customer_id,
        ${MULTIIF_CAT} >= 'c' AND value > 12, customer_id,
        ${MULTIIF_CAT} >= 'd' AND value > 13, customer_id,
        ''
    )),
    sum(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 0, value,
        ${MULTIIF_CAT} >= 'b' AND value > 1, value,
        ${MULTIIF_CAT} >= 'c' AND value > 2, value,
        ${MULTIIF_CAT} >= 'd' AND value > 3, value,
        0
    )),
    sum(multiIf(
        ${MULTIIF_CAT} >= 'e' AND value > 0, value,
        ${MULTIIF_CAT} >= 'f' AND value > 1, value,
        ${MULTIIF_CAT} >= 'g' AND value > 2, value,
        ${MULTIIF_CAT} >= 'h' AND value > 3, value,
        0
    )),
    uniqExact(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 100, customer_id,
        ${MULTIIF_CAT} >= 'b' AND value > 101, customer_id,
        ${MULTIIF_CAT} >= 'c' AND value > 102, customer_id,
        ${MULTIIF_CAT} >= 'd' AND value > 103, customer_id,
        ''
    )),
    sum(toInt64(multiIf(
        ${MULTIIF_CAT} >= 'a', value * 2,
        ${MULTIIF_CAT} >= 'b', value * 3,
        ${MULTIIF_CAT} >= 'c', value * 4,
        ${MULTIIF_CAT} >= 'd', value * 5,
        0
    )))
FROM ${CLICKHOUSE_DATABASE}.t_merge_perf_all
WHERE
    date >= '2021-10-04' AND date <= '2021-12-13'
    AND ${MULTIIF_CAT} >= 'a'
    AND ${MULTIIF_CAT} <= 'z'
    AND multiIf(
        position(category, 'auto') > 0, 1,
        position(category, 'appliances') > 0, 2,
        position(category, 'garden') > 0, 3,
        position(category, 'children') > 0, 4,
        position(category, 'home') > 0, 5,
        position(category, 'hobbies') > 0, 6,
        position(category, 'electronics') > 0, 7,
        position(category, 'books') > 0, 8,
        position(category, 'computers') > 0, 9,
        position(category, 'beauty') > 0, 10,
        position(category, 'equipment') > 0, 11,
        position(category, 'clothing') > 0, 12,
        position(category, 'food') > 0, 13,
        position(category, 'sports') > 0, 14,
        position(category, 'construction') > 0, 15,
        position(category, 'health') > 0, 16,
        0
    ) >= 1
    AND multiIf(
        position(category, 'auto') > 0, 1,
        position(category, 'appliances') > 0, 2,
        position(category, 'garden') > 0, 3,
        position(category, 'children') > 0, 4,
        position(category, 'home') > 0, 5,
        position(category, 'hobbies') > 0, 6,
        position(category, 'electronics') > 0, 7,
        position(category, 'books') > 0, 8,
        position(category, 'computers') > 0, 9,
        position(category, 'beauty') > 0, 10,
        position(category, 'equipment') > 0, 11,
        position(category, 'clothing') > 0, 12,
        position(category, 'food') > 0, 13,
        position(category, 'sports') > 0, 14,
        position(category, 'construction') > 0, 15,
        position(category, 'health') > 0, 16,
        0
    ) <= 16
GROUP BY
    addDays(CAST(date, 'Date'), -1 * (((7 + if(toDayOfWeek(date) = 7, 1, toDayOfWeek(date) + 1)) - 2) % 7))
FORMAT Null"

# Run query on merge table with a 10-second timeout.
# Before the optimization this took ~0.65s with 100 tables.
# With the optimization it should complete well under 10 seconds.
$CLICKHOUSE_CLIENT --max_query_size 1048576 --max_execution_time 10 -q "$QUERY" && echo "OK" || echo "FAIL: query on merge table timed out"

# Also verify correctness: same query on single table vs merge table should produce equal results.
QUERY_RESULT="SELECT
    count(),
    sum(value),
    uniqExact(customer_id)
FROM ${CLICKHOUSE_DATABASE}.t_merge_perf_all
WHERE
    date >= '2021-10-04' AND date <= '2021-12-13'
    AND multiIf(
        position(category, 'auto') > 0, 1,
        position(category, 'appliances') > 0, 2,
        position(category, 'garden') > 0, 3,
        position(category, 'children') > 0, 4,
        0
    ) >= 1"

QUERY_SINGLE="${QUERY_RESULT/t_merge_perf_all/t_merge_perf_0}"

RESULT_MERGE=$($CLICKHOUSE_CLIENT -q "$QUERY_RESULT")
RESULT_SINGLE=$($CLICKHOUSE_CLIENT -q "$QUERY_SINGLE")

if [ "$RESULT_MERGE" = "$RESULT_SINGLE" ]; then
    echo "OK"
else
    echo "FAIL: merge table result differs from single table"
    echo "Merge: $RESULT_MERGE"
    echo "Single: $RESULT_SINGLE"
fi

# Cleanup
for i in $(seq 0 $((NUM_TABLES - 1))); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_merge_perf_${i}"
done
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_merge_perf_all"
