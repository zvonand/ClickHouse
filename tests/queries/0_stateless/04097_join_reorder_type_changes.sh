#!/usr/bin/env bash
# Regression test: join reorder with type-changing joins (e.g. LEFT JOIN + join_use_nulls)
# could cause "Cannot fold actions for projection" when the optimizer separates a relation
# from the join that causes its type change.
# We test multiple seeds to ensure the bug is triggered regardless of join order.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t1 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t2 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t3 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;

    INSERT INTO ${CLICKHOUSE_DATABASE}.t1 VALUES (1, 'a'), (2, 'b');
    INSERT INTO ${CLICKHOUSE_DATABASE}.t2 VALUES (1, 'c'), (3, 'd');
    INSERT INTO ${CLICKHOUSE_DATABASE}.t3 VALUES (1, 'e'), (2, 'f');
"

for seed in $(seq 1 50); do
    $CLICKHOUSE_CLIENT -q "
        SELECT t1.id, t2.id, t3.id
        FROM ${CLICKHOUSE_DATABASE}.t1
        LEFT JOIN ${CLICKHOUSE_DATABASE}.t2 ON t1.id = t2.id
        INNER JOIN ${CLICKHOUSE_DATABASE}.t3 ON t2.id = t3.id
        ORDER BY ALL
        SETTINGS join_use_nulls = 1,
            query_plan_optimize_join_order_limit = 10,
            query_plan_optimize_join_order_randomize = $seed
    " 2>&1
done | sort -u

$CLICKHOUSE_CLIENT -q "
    DROP TABLE ${CLICKHOUSE_DATABASE}.t1;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t2;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t3;
"
