-- Tags: no-replicated-database
-- no-replicated-database: TRUNCATE ALL TABLES is not supported for Replicated databases.

-- Regression test: TRUNCATE ALL TABLES should skip views that don't support truncation.
-- https://github.com/ClickHouse/ClickHouse/issues/78165

DROP TABLE IF EXISTS truncate_test_data;
DROP VIEW IF EXISTS truncate_test_view;

CREATE TABLE truncate_test_data (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO truncate_test_data VALUES (1, 'Alice'), (2, 'Bob');

CREATE VIEW truncate_test_view AS SELECT * FROM truncate_test_data;

SELECT count() FROM truncate_test_data;
SELECT count() FROM truncate_test_view;

TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier};

SELECT count() FROM truncate_test_data;
SELECT count() FROM truncate_test_view;

-- Also test with LIKE pattern
INSERT INTO truncate_test_data VALUES (3, 'Charlie');

SELECT count() FROM truncate_test_data;

TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier} LIKE '%';

SELECT count() FROM truncate_test_data;
SELECT count() FROM truncate_test_view;

DROP VIEW truncate_test_view;
DROP TABLE truncate_test_data;
