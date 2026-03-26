-- Tags: no-fasttest
-- Test that merge() function works with remote/distributed tables that have different schemas.
-- https://github.com/ClickHouse/ClickHouse/issues/86393

SET enable_analyzer = 1;

DROP TABLE IF EXISTS r_t1;
DROP TABLE IF EXISTS r_t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a Int64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 10), (2, 20);
INSERT INTO t2 VALUES (3);

-- Works with local MergeTree tables.
SELECT uniq(b) FROM merge(currentDatabase(), 't[12]') ORDER BY ALL;
SELECT sum(b) FROM merge(currentDatabase(), 't[12]');
SELECT a, b FROM merge(currentDatabase(), 't[12]') ORDER BY a;

-- Should also work with remote() table wrappers.
CREATE TABLE r_t1 AS remote('127.0.0.1', currentDatabase(), 't1');
CREATE TABLE r_t2 AS remote('127.0.0.1', currentDatabase(), 't2');

SELECT uniq(b) FROM merge(currentDatabase(), 'r_t[12]') ORDER BY ALL;
SELECT sum(b) FROM merge(currentDatabase(), 'r_t[12]');
SELECT a, b FROM merge(currentDatabase(), 'r_t[12]') ORDER BY a;

DROP TABLE r_t1;
DROP TABLE r_t2;
DROP TABLE t1;
DROP TABLE t2;
