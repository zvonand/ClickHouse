SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (a UInt64, b String, c Float64, d Nullable(UInt64), dt DateTime) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t1 SELECT number, toString(number), number * 1.5, if(number % 3 = 0, NULL, number), '2024-06-15 12:00:00' FROM numbers(100);

SELECT '--- Arithmetic operators in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a + 1 > 5 AND a - 2 < 100 AND a * 3 != 0 AND a / 2 > 1 AND a % 5 = 0 AND intDiv(a, 6) > 0;

SELECT '--- Unary negate in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE -a < -5;

SELECT '--- Comparison operators ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a > 5 AND a != 10 AND c <= 3.0;

SELECT '--- Logical AND OR NOT ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE (a > 1 OR a < 10) AND NOT (b = 'x');

SELECT '--- Precedence: OR inside AND needs parens ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE (a = 1 OR a = 2) AND b = 'x';

SELECT '--- IS NULL / IS NOT NULL ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE d IS NULL;

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE d IS NOT NULL;

SELECT '--- LIKE / NOT LIKE / ILIKE ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE b LIKE '%foo%';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE b NOT LIKE '%bar%';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE b ILIKE '%BAZ%';

SELECT '--- REGEXP ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE match(b, '^[0-9]+$');

SELECT '--- CAST in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE CAST(a AS String) = '5';

SELECT '--- Concat || in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE b || '_suffix' = '5_suffix';

SELECT '--- tupleElement in GROUP BY ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT tuple(a, b).1, count() FROM t1 GROUP BY tuple(a, b).1;

SELECT '--- arrayElement in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE [a, c][1] > 5;

SELECT '--- arrayJoin in GROUP BY ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT count() FROM t1 GROUP BY arrayJoin([a, a + 1, a + 2]);

SELECT '--- DateTime constant in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE dt > '2024-01-01 00:00:00';

SELECT '--- Nested arithmetic precedence in filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE (a + 1) * 2 > 10 AND a + 1 * 2 < 100;

DROP TABLE t1;
