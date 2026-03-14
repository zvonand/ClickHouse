-- Tags: no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99326
-- Avro output should throw BAD_ARGUMENTS instead of logical error
-- when an Enum column contains a value not in the enum definition.

-- Test Enum8: insert valid value, then narrow enum definition via ALTER
DROP TABLE IF EXISTS enum8_test;
CREATE TABLE enum8_test (e Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO enum8_test VALUES ('c');
ALTER TABLE enum8_test MODIFY COLUMN e Enum8('a' = 1, 'b' = 2);
SELECT * FROM enum8_test FORMAT Avro; -- { clientError BAD_ARGUMENTS }
DROP TABLE enum8_test;

-- Test Enum16: same approach
DROP TABLE IF EXISTS enum16_test;
CREATE TABLE enum16_test (e Enum16('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO enum16_test VALUES ('c');
ALTER TABLE enum16_test MODIFY COLUMN e Enum16('a' = 1, 'b' = 2);
SELECT * FROM enum16_test FORMAT Avro; -- { clientError BAD_ARGUMENTS }
DROP TABLE enum16_test;
