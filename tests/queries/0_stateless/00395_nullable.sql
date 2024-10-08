SELECT '----- NULL value -----';

SELECT NULL;
SELECT 1 + NULL;
SELECT abs(NULL);
SELECT NULL + NULL;

SELECT '----- MergeTree engine -----';

DROP TABLE IF EXISTS test1_00395;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE test1_00395(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = MergeTree(d, (col1, d), 8192);

INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;


SELECT '----- Memory engine -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = Memory;

INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- TinyLog engine -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = TinyLog;

INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- Log engine -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = Log;

INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- StripeLog engine -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = StripeLog;

INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01'),
                               (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;


SELECT '----- Insert with expression -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt64))) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES ([1+1]);
SELECT col1 FROM test1_00395 ORDER BY col1 ASC;

SELECT '----- Insert. Source and target columns have same types up to nullability. -----';
DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(UInt64), col2 UInt64) Engine=Memory;
DROP TABLE IF EXISTS test2;
CREATE TABLE test2(col1 UInt64, col2 Nullable(UInt64)) Engine=Memory;
INSERT INTO test1_00395(col1,col2) VALUES (2,7)(6,9)(5,1)(4,3)(8,2);
INSERT INTO test2(col1,col2) SELECT col1,col2 FROM test1_00395;
SELECT col1,col2 FROM test2 ORDER BY col1,col2 ASC;

SELECT '----- Apply functions and aggregate functions on columns that may contain null values -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(UInt64), col2 Nullable(UInt64)) Engine=Memory;
INSERT INTO test1_00395(col1,col2) VALUES (2,7)(NULL,6)(9,NULL)(NULL,NULL)(5,1)(42,42);
SELECT col1, col2, col1 + col2, col1 * 7 FROM test1_00395 ORDER BY col1,col2 ASC;
SELECT sum(col1) FROM test1_00395;
SELECT sum(col1 * 7) FROM test1_00395;

SELECT '----- isNull, isNotNull -----';

SELECT col1, col2, isNull(col1), isNotNull(col2) FROM test1_00395 ORDER BY col1,col2 ASC;

SELECT '----- ifNull, nullIf -----';

SELECT col1, col2, ifNull(col1,col2) FROM test1_00395 ORDER BY col1,col2 ASC;
SELECT col1, col2, nullIf(col1,col2) FROM test1_00395 ORDER BY col1,col2 ASC;
SELECT nullIf(1, NULL);

SELECT '----- coalesce -----';

SELECT coalesce(NULL);
SELECT coalesce(NULL, 1);
SELECT coalesce(NULL, NULL, 1);
SELECT coalesce(NULL, 42, NULL, 1);
SELECT coalesce(NULL, NULL, NULL);
SELECT col1, col2, coalesce(col1, col2) FROM test1_00395 ORDER BY col1, col2 ASC;
SELECT col1, col2, coalesce(col1, col2, 99) FROM test1_00395 ORDER BY col1, col2 ASC;

SELECT '----- assumeNotNull -----';

SELECT res FROM (SELECT col1, assumeNotNull(col1) AS res FROM test1_00395) WHERE col1 IS NOT NULL ORDER BY res ASC;

SELECT '----- IS NULL, IS NOT NULL -----';

SELECT col1 FROM test1_00395 WHERE col1 IS NOT NULL ORDER BY col1 ASC;
SELECT col1 FROM test1_00395 WHERE col1 IS NULL;

SELECT '----- if -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395 (col1 Nullable(String)) ENGINE=TinyLog;
INSERT INTO test1_00395 VALUES ('a'), ('b'), ('c'), (NULL);

SELECT col1, if(col1 IN ('a' ,'b'), 1, 0) AS t, toTypeName(t) FROM test1_00395;
SELECT col1, if(col1 IN ('a' ,'b'), NULL, 0) AS t, toTypeName(t) FROM test1_00395;

SELECT '----- case when -----';

SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN 1 ELSE 0 END AS t, toTypeName(t) FROM test1_00395;
SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN NULL ELSE 0 END AS t, toTypeName(t) FROM test1_00395;
SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN 1 END AS t, toTypeName(t) FROM test1_00395;

SELECT '----- multiIf -----';

SELECT multiIf(1, NULL, 1, 3, 4);
SELECT multiIf(1, 2, 1, NULL, 4);
SELECT multiIf(NULL, NULL, NULL);

SELECT multiIf(1, 'A', 1, NULL, 'DEF');
SELECT multiIf(1, toFixedString('A', 16), 1, NULL, toFixedString('DEF', 16));

SELECT multiIf(NULL, 2, 1, 3, 4);
SELECT multiIf(1, 2, NULL, 3, 4);

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(Int8), col2 Nullable(UInt16), col3 Nullable(Float32)) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2,col3) VALUES (toInt8(1),toUInt16(2),toFloat32(3))(NULL,toUInt16(1),toFloat32(2))(toInt8(1),NULL,toFloat32(2))(toInt8(1),toUInt16(2),NULL);
SELECT multiIf(col1 == 1, col2, col2 == 2, col3, col3 == 3, col1, 42) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(cond1 Nullable(UInt8), then1 Int8, cond2 UInt8, then2 Nullable(UInt16), then3 Nullable(Float32)) Engine=TinyLog;
INSERT INTO test1_00395(cond1,then1,cond2,then2,then3) VALUES(1,1,1,42,99)(0,7,1,99,42)(NULL,6,2,99,NULL);
SELECT multiIf(cond1,then1,cond2,then2,then3) FROM test1_00395;

SELECT '----- Array functions -----';

SELECT [NULL];
SELECT [NULL,NULL,NULL];
SELECT [NULL,2,3];
SELECT [1,NULL,3];
SELECT [1,2,NULL];

SELECT [NULL,'b','c'];
SELECT ['a',NULL,'c'];
SELECT ['a','b',NULL];

SELECT '----- arrayElement -----';

SELECT '----- constant arrays -----';

SELECT arrayElement([1,NULL,2,3], 1);
SELECT arrayElement([1,NULL,2,3], 2);
SELECT arrayElement([1,NULL,2,3], 3);
SELECT arrayElement([1,NULL,2,3], 4);

SELECT arrayElement(['a',NULL,'c','d'], 1);
SELECT arrayElement(['a',NULL,'c','d'], 2);
SELECT arrayElement(['a',NULL,'c','d'], 3);
SELECT arrayElement(['a',NULL,'c','d'], 4);

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 UInt64) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES(1),(2),(3),(4);

SELECT arrayElement([1,NULL,2,3], col1) FROM test1_00395;

SELECT '----- variable arrays -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt64))) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES([2,3,7,NULL]),
                                    ([NULL,3,7,4]),
                                    ([2,NULL,7,NULL]),
                                    ([2,3,NULL,4]),
                                    ([NULL,NULL,NULL,NULL]);

SELECT arrayElement(col1, 1) FROM test1_00395;
SELECT arrayElement(col1, 2) FROM test1_00395;
SELECT arrayElement(col1, 3) FROM test1_00395;
SELECT arrayElement(col1, 4) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(String))) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES(['a','bc','def',NULL]),
                                    ([NULL,'bc','def','ghij']),
                                    (['a',NULL,'def',NULL]),
                                    (['a','bc',NULL,'ghij']),
                                    ([NULL,NULL,NULL,NULL]);

SELECT arrayElement(col1, 1) FROM test1_00395;
SELECT arrayElement(col1, 2) FROM test1_00395;
SELECT arrayElement(col1, 3) FROM test1_00395;
SELECT arrayElement(col1, 4) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt64)), col2 UInt64) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES([2,3,7,NULL], 1),
                                         ([NULL,3,7,4], 2),
                                         ([2,NULL,7,NULL], 3),
                                         ([2,3,NULL,4],4),
                                         ([NULL,NULL,NULL,NULL],3);

SELECT arrayElement(col1,col2) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(String)), col2 UInt64) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc','def',NULL], 1),
                                         ([NULL,'bc','def','ghij'], 2),
                                         (['a',NULL,'def','ghij'], 3),
                                         (['a','bc',NULL,'ghij'],4),
                                         ([NULL,NULL,NULL,NULL],3);

SELECT arrayElement(col1,col2) FROM test1_00395;

SELECT '----- has -----';

SELECT '----- constant arrays -----';

SELECT has([1,NULL,2,3], 1);
SELECT has([1,NULL,2,3], NULL);
SELECT has([1,NULL,2,3], 2);
SELECT has([1,NULL,2,3], 3);
SELECT has([1,NULL,2,3], 4);

SELECT has(['a',NULL,'def','ghij'], 'a');
SELECT has(['a',NULL,'def','ghij'], NULL);
SELECT has(['a',NULL,'def','ghij'], 'def');
SELECT has(['a',NULL,'def','ghij'], 'ghij');

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 UInt64) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES(1),(2),(3),(4);

SELECT has([1,NULL,2,3], col1) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(UInt64)) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES(1),(2),(3),(4),(NULL);

SELECT has([1,NULL,2,3], col1) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 String) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES('a'),('bc'),('def'),('ghij');

SELECT has(['a',NULL,'def','ghij'], col1) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(String)) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES('a'),('bc'),('def'),('ghij'),(NULL);

SELECT has(['a',NULL,'def','ghij'], col1) FROM test1_00395;

SELECT '----- variable arrays -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt64))) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES([2,3,7,NULL]),
                                    ([NULL,3,7,4]),
                                    ([2,NULL,7,NULL]),
                                    ([2,3,NULL,4]),
                                    ([NULL,NULL,NULL,NULL]);

SELECT has(col1, 2) FROM test1_00395;
SELECT has(col1, 3) FROM test1_00395;
SELECT has(col1, 4) FROM test1_00395;
SELECT has(col1, 5) FROM test1_00395;
SELECT has(col1, 7) FROM test1_00395;
SELECT has(col1, NULL) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(String))) Engine=TinyLog;
INSERT INTO test1_00395(col1) VALUES(['a','bc','def',NULL]),
                                    ([NULL,'bc','def','ghij']),
                                    (['a',NULL,'def',NULL]),
                                    (['a','bc',NULL,'ghij']),
                                    ([NULL,NULL,NULL,NULL]);

SELECT has(col1, 'a') FROM test1_00395;
SELECT has(col1, 'bc') FROM test1_00395;
SELECT has(col1, 'def') FROM test1_00395;
SELECT has(col1, 'ghij') FROM test1_00395;
SELECT has(col1,  NULL) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt64)), col2 UInt64) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES([2,3,7,NULL], 2),
                                         ([NULL,3,7,4], 3),
                                         ([2,NULL,7,NULL], 7),
                                         ([2,3,NULL,4],5);

SELECT has(col1,col2) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt64)), col2 Nullable(UInt64)) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES([2,3,7,NULL], 2),
                                         ([NULL,3,7,4], 3),
                                         ([2,NULL,7,NULL], 7),
                                         ([2,3,NULL,4],5),
                                         ([NULL,NULL,NULL,NULL],NULL);

SELECT has(col1,col2) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(String)), col2 String) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc','def',NULL], 'a'),
                                         ([NULL,'bc','def','ghij'], 'bc'),
                                         (['a',NULL,'def','ghij'], 'def'),
                                         (['a','bc',NULL,'ghij'], 'ghij');

SELECT has(col1,col2) FROM test1_00395;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(String)), col2 Nullable(String)) Engine=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc','def',NULL], 'a'),
                                         ([NULL,'bc','def','ghij'], 'bc'),
                                         (['a',NULL,'def','ghij'], 'def'),
                                         (['a','bc',NULL,'ghij'], 'ghij'),
                                         ([NULL,NULL,NULL,NULL], NULL);

SELECT has(col1,col2) FROM test1_00395;

SELECT '----- Aggregation -----';

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(String), col2 Nullable(UInt8), col3 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 0, 'ABCDEFGH'),
                                              ('A', 0, 'BACDEFGH'),
                                              ('A', 1, 'BCADEFGH'),
                                              ('A', 1, 'BCDAEFGH'),
                                              ('B', 1, 'BCDEAFGH'),
                                              ('B', 1, 'BCDEFAGH'),
                                              ('B', 1, 'BCDEFGAH'),
                                              ('B', 1, 'BCDEFGHA'),
                                              ('C', 1, 'ACBDEFGH'),
                                              ('C', NULL, 'ACDBEFGH'),
                                              ('C', NULL, 'ACDEBFGH'),
                                              ('C', NULL, 'ACDEFBGH'),
                                              (NULL, 1, 'ACDEFGBH'),
                                              (NULL, NULL, 'ACDEFGHB');

SELECT col1, col2, count() FROM test1_00395 GROUP BY col1, col2 ORDER BY col1, col2;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 String, col2 Nullable(UInt8), col3 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 0, 'ABCDEFGH'),
                                              ('A', 0, 'BACDEFGH'),
                                              ('A', 1, 'BCADEFGH'),
                                              ('A', 1, 'BCDAEFGH'),
                                              ('B', 1, 'BCDEAFGH'),
                                              ('B', 1, 'BCDEFAGH'),
                                              ('B', 1, 'BCDEFGAH'),
                                              ('B', 1, 'BCDEFGHA'),
                                              ('C', 1, 'ACBDEFGH'),
                                              ('C', NULL, 'ACDBEFGH'),
                                              ('C', NULL, 'ACDEBFGH'),
                                              ('C', NULL, 'ACDEFBGH');

SELECT col1, col2, count() FROM test1_00395 GROUP BY col1, col2 ORDER BY col1, col2;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(String), col2 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES('A', 'ABCDEFGH'),
                                         ('A', 'BACDEFGH'),
                                         ('A', 'BCADEFGH'),
                                         ('A', 'BCDAEFGH'),
                                         ('B', 'BCDEAFGH'),
                                         ('B', 'BCDEFAGH'),
                                         ('B', 'BCDEFGAH'),
                                         ('B', 'BCDEFGHA'),
                                         ('C', 'ACBDEFGH'),
                                         ('C', 'ACDBEFGH'),
                                         ('C', 'ACDEBFGH'),
                                         ('C', 'ACDEFBGH'),
                                         (NULL, 'ACDEFGBH'),
                                         (NULL, 'ACDEFGHB');

SELECT col1, count() FROM test1_00395 GROUP BY col1 ORDER BY col1;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(UInt8), col2 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES(0, 'ABCDEFGH'),
                                         (0, 'BACDEFGH'),
                                         (1, 'BCADEFGH'),
                                         (1, 'BCDAEFGH'),
                                         (1, 'BCDEAFGH'),
                                         (1, 'BCDEFAGH'),
                                         (1, 'BCDEFGAH'),
                                         (1, 'BCDEFGHA'),
                                         (1, 'ACBDEFGH'),
                                         (NULL, 'ACDBEFGH'),
                                         (NULL, 'ACDEBFGH'),
                                         (NULL, 'ACDEFBGH');

SELECT col1, count() FROM test1_00395 GROUP BY col1 ORDER BY col1;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(UInt64), col2 UInt64, col3 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2,col3) VALUES(0, 2, 'ABCDEFGH'),
                                              (0, 3, 'BACDEFGH'),
                                              (1, 5, 'BCADEFGH'),
                                              (1, 2, 'BCDAEFGH'),
                                              (1, 3, 'BCDEAFGH'),
                                              (1, 5, 'BCDEFAGH'),
                                              (1, 2, 'BCDEFGAH'),
                                              (1, 3, 'BCDEFGHA'),
                                              (1, 5, 'ACBDEFGH'),
                                              (NULL, 2, 'ACDBEFGH'),
                                              (NULL, 3, 'ACDEBFGH'),
                                              (NULL, 3, 'ACDEFBGH');

SELECT col1, col2, count() FROM test1_00395 GROUP BY col1, col2 ORDER BY col1, col2;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Nullable(UInt64), col2 UInt64, col3 Nullable(UInt64), col4 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(0, 2, 1, 'ABCDEFGH'),
                                                   (0, 3, NULL, 'BACDEFGH'),
                                                   (1, 5, 1, 'BCADEFGH'),
                                                   (1, 2, NULL, 'BCDAEFGH'),
                                                   (1, 3, 1, 'BCDEAFGH'),
                                                   (1, 5, NULL, 'BCDEFAGH'),
                                                   (1, 2, 1, 'BCDEFGAH'),
                                                   (1, 3, NULL, 'BCDEFGHA'),
                                                   (1, 5, 1, 'ACBDEFGH'),
                                                   (NULL, 2, NULL, 'ACDBEFGH'),
                                                   (NULL, 3, 1, 'ACDEBFGH'),
                                                   (NULL, 3, NULL, 'ACDEFBGH');

SELECT col1, col2, col3, count() FROM test1_00395 GROUP BY col1, col2, col3 ORDER BY col1, col2, col3;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395(col1 Array(Nullable(UInt8)), col2 String) ENGINE=TinyLog;
INSERT INTO test1_00395(col1,col2) VALUES([0], 'ABCDEFGH'),
                                         ([0], 'BACDEFGH'),
                                         ([1], 'BCADEFGH'),
                                         ([1], 'BCDAEFGH'),
                                         ([1], 'BCDEAFGH'),
                                         ([1], 'BCDEFAGH'),
                                         ([1], 'BCDEFGAH'),
                                         ([1], 'BCDEFGHA'),
                                         ([1], 'ACBDEFGH'),
                                         ([NULL], 'ACDBEFGH'),
                                         ([NULL], 'ACDEBFGH'),
                                         ([NULL], 'ACDEFBGH');

SELECT col1, count() FROM test1_00395 GROUP BY col1 ORDER BY col1;

DROP TABLE IF EXISTS test1_00395;
DROP TABLE test2;
