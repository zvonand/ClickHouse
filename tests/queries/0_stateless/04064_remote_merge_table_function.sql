-- Tags: shard
-- Test that remote() with nested merge() table function works with the analyzer.
-- https://github.com/ClickHouse/ClickHouse/issues/84672

DROP TABLE IF EXISTS test_t1;
DROP TABLE IF EXISTS test_t2;

CREATE TABLE test_t1 (x UInt64) ENGINE = Memory;
CREATE TABLE test_t2 (x UInt64) ENGINE = Memory;

INSERT INTO test_t1 VALUES (1), (2);
INSERT INTO test_t2 VALUES (3), (4);

-- merge() nested inside remote() should be sent to the remote server, not resolved locally.
SELECT sum(x) FROM remote('127.0.0.1', merge(currentDatabase(), '^test_t'));

-- Verify that merge() is kept as a TABLE_FUNCTION node in the query tree (not resolved locally).
-- On master without the fix, merge() would be resolved to a TABLE node during analysis.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(x) FROM remote('127.0.0.1', merge(currentDatabase(), '^test_t'))) WHERE explain LIKE '%table\_function\_name: merge%';

DROP TABLE test_t1;
DROP TABLE test_t2;
