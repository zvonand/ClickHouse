-- The optimization rewrites `SELECT agg() FROM t GROUP BY k LIMIT n`
-- into the same query with `max_rows_to_group_by = n` and
-- `group_by_overflow_mode = 'any'`, so the aggregation stops as soon as
-- `n` distinct keys are produced instead of grouping the full input.

DROP TABLE IF EXISTS t_trivial_group_by_limit;

CREATE TABLE t_trivial_group_by_limit (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_trivial_group_by_limit SELECT number FROM numbers(1000000);

-- With the optimization on or off, the projection always returns exactly LIMIT rows,
-- so this query reports the same value either way; the difference is internal — the
-- optimization stops the aggregation once 5 distinct keys are produced.
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 0;

-- Optimization should not fire when HAVING / ORDER BY are present.
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k HAVING k > 0 LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k ORDER BY k LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 1;

-- Optimization is suppressed when the user already set `max_rows_to_group_by`
-- (it would otherwise overwrite the user setting).
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5)
SETTINGS optimize_trivial_group_by_limit_query = 1, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw';

-- Overflow guard: `limit + offset` must not silently wrap to a smaller number.
-- Without the guard, `LIMIT 18446744073709551615 OFFSET 100` would overflow `UInt64`
-- to 99, truncating aggregation to 99 distinct keys and yielding 0 instead of 156.
SELECT count() FROM (SELECT number % 256 AS k FROM numbers(1000) GROUP BY k LIMIT 18446744073709551615 OFFSET 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;

-- Optimization must not fire for `GROUP BY` modifiers (`WITH ROLLUP`, `WITH CUBE`,
-- `WITH GROUPING SETS`, `WITH TOTALS`): forcing `max_rows_to_group_by` with
-- `group_by_overflow_mode = 'any'` would silently drop groups and corrupt the
-- subtotal/total rows produced by these modifiers.
-- Each of the queries below has 5 distinct keys plus the modifier's extra rows.

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH ROLLUP LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH ROLLUP LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH CUBE LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH CUBE LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY GROUPING SETS ((k), ()) LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY GROUPING SETS ((k), ()) LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH TOTALS LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH TOTALS LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

DROP TABLE t_trivial_group_by_limit;
