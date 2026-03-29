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

DROP TABLE test_t1;
DROP TABLE test_t2;
