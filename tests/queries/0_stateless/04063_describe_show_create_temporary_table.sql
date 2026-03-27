-- Test for https://github.com/ClickHouse/ClickHouse/issues/14004
-- SHOW CREATE TABLE and DESCRIBE TABLE should be consistent with respect to TEMPORARY tables.

SET describe_compact_output = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (hello String) ENGINE = MergeTree ORDER BY hello;
CREATE TEMPORARY TABLE t (world UInt64);

-- Both SHOW CREATE TABLE and DESCRIBE TABLE should prefer the temporary table
-- when no database is specified.
SELECT 'SHOW CREATE TABLE (no database qualifier) should show temporary table:';
SHOW CREATE TABLE t;

SELECT 'DESCRIBE TABLE (no database qualifier) should show temporary table:';
DESCRIBE TABLE t;

-- DESCRIBE TEMPORARY TABLE should work (was a syntax error before the fix).
SELECT 'DESCRIBE TEMPORARY TABLE:';
DESCRIBE TEMPORARY TABLE t;

-- SHOW CREATE TEMPORARY TABLE should still work.
SELECT 'SHOW CREATE TEMPORARY TABLE:';
SHOW CREATE TEMPORARY TABLE t;

DROP TABLE t; -- drops permanent table
DROP TEMPORARY TABLE t;
