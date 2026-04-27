DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)) ENGINE = GenerateRandom();
SELECT COUNT(*) FROM (SELECT * FROM test_table LIMIT 100);

DROP TABLE IF EXISTS test_table;

SELECT '-';

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2(a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3, 'UTC'), UUID)) ENGINE = GenerateRandom(10, 5, 3);

SELECT * FROM test_table_2 LIMIT 100;

SELECT '-';

DROP TABLE IF EXISTS test_table_2;

SELECT '-';

CREATE TABLE test_table_3 (s String) ENGINE = GenerateRandom(1, 100);

SELECT max(length(s)) > 1 AS has_long_strings FROM (SELECT s FROM test_table_3 LIMIT 1000);

DROP TABLE IF EXISTS test_table_3;
