-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/100614
-- Async insert should respect session_timezone when parsing DateTime from string literals.

DROP TABLE IF EXISTS test_async_tz;
CREATE TABLE test_async_tz (d DateTime) ENGINE = Memory;

-- Setting session_timezone after table creation is important to reproduce the bug:
-- the async insert flush context must carry the session_timezone from the INSERT,
-- not from the CREATE TABLE.
SET session_timezone = 'Asia/Novosibirsk';

-- Insert with async_insert enabled and wait for flush.
INSERT INTO test_async_tz SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2000-01-01 01:00:00');

-- Insert with async_insert disabled (known-good path) for comparison.
INSERT INTO test_async_tz SETTINGS async_insert = 0 VALUES ('2000-01-01 01:00:00');

-- Both rows should be identical: the async path must produce the same value as the sync path.
SELECT count() FROM test_async_tz;

-- Explicit check: the stored values must be the same.
SELECT uniqExact(d) FROM test_async_tz;

SELECT d FROM test_async_tz;

DROP TABLE test_async_tz;
