DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt32, date DateTime, test_enum Enum8('system' = 1)) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (date);
INSERT INTO test VALUES (1, '2026-01-01 00:00:00', 'system');
SELECT id, IF(test_enum = 1, 0, test_enum) AS test_enum FROM test WHERE IF(test_enum = 1, 1, test_enum) != 0 ORDER BY date LIMIT 1;
DROP TABLE test;
