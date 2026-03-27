-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/44301 and https://github.com/ClickHouse/ClickHouse/issues/50382

DROP TABLE IF EXISTS test_table_dist;
DROP TABLE IF EXISTS test_table;
DROP DICTIONARY IF EXISTS test_dict;

CREATE DICTIONARY test_dict (id UInt64, val UInt64)
PRIMARY KEY id
LAYOUT(FLAT)
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS id, number AS val FROM numbers(100)'))
LIFETIME(0);

CREATE TABLE test_table ENGINE = Log AS SELECT number FROM numbers(200);

CREATE TABLE test_table_dist ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_table) AS test_table;

SELECT number, dictGet('test_dict', 'val', toUInt64(number)) FROM test_table_dist ORDER BY number;

DROP TABLE test_table_dist;
DROP TABLE test_table;
DROP DICTIONARY test_dict;
