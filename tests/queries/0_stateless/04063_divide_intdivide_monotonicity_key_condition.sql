-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

DROP TABLE IF EXISTS test_int_divide;
CREATE TABLE test_int_divide (x Int32) ENGINE = MergeTree PARTITION BY x ORDER BY tuple();
INSERT INTO test_int_divide VALUES (0), (1), (2), (3), (4);

SELECT * FROM test_int_divide WHERE x != 0 AND intDiv(10, x) > 4 ORDER BY x;

EXPLAIN indexes = 1
SELECT * FROM test_int_divide WHERE x != 0 AND intDiv(10, x) > 4;

DROP TABLE IF EXISTS test_divide;
CREATE TABLE test_divide (x Decimal32(2)) ENGINE = MergeTree PARTITION BY x ORDER BY tuple();
INSERT INTO test_divide VALUES (0), (1), (2);

SELECT * FROM test_divide WHERE x != 0 AND divide(10, x) > 4 ORDER BY x;

EXPLAIN indexes = 1
SELECT * FROM test_divide WHERE x != 0 AND divide(10, x) > 4 ORDER BY x;
