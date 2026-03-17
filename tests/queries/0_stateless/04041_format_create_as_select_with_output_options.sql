-- Verify that CREATE TABLE ... AS SELECT round-trips correctly when trailing
-- output options (FORMAT, SETTINGS) are present on an outer query like EXPLAIN.
-- The formatter must parenthesize the AS-select so that the output options are
-- not consumed by the inner SELECT during re-parsing.

-- EXPLAIN CREATE TABLE ... AS SELECT ... FORMAT: parentheses required
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');

-- Round-trip: formatting twice must produce the same result
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
     = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));

-- Without output options: no parentheses
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2');

-- CREATE TABLE with its own SETTINGS and output SETTINGS
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
     = formatQuery(formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));
