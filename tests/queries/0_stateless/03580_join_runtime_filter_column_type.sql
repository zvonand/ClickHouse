-- Repro for https://github.com/ClickHouse/ClickHouse/issues/89062
CREATE TABLE t0 (c0 Int) ENGINE = Memory;
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT 1 FROM t0 WHERE EXISTS (SELECT t0._table) SETTINGS enable_join_runtime_filters = 1, allow_experimental_correlated_subqueries = 1;
