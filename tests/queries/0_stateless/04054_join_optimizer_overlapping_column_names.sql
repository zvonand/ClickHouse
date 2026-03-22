-- Regression test: join order optimizer must not crash (LOGICAL_ERROR) when
-- left and right sides of a join have overlapping internal column names.
-- This can happen when scalar subquery results and join table aliases collide.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100398&sha=d69b29735a13a4e8b1c0b95f263a4d962a8943da&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29

DROP TABLE IF EXISTS t_04054;
CREATE TABLE t_04054 (part_date Date, pk_date Date) ENGINE = MergeTree ORDER BY pk_date PARTITION BY part_date;

-- This is a simplified version of the fuzzed query that triggered the exception.
-- The key ingredients: scalar subqueries in WHERE that produce internal column names
-- colliding with the right side of a NOT IN join.
SELECT materialize(toInt256(-2147483649))
FROM t_04054
WHERE (intDiv(multiIf(
    lessOrEquals(moduloOrZero(intDivOrZero(minus(NULL, divide(
        intDivOrZero(NULL, minus(NULL, intDiv((SELECT DISTINCT NULL LIMIT 65536), '\0'))),
        concat(toInt64(plus(divide(intDiv(NULL, moduloOrZero(NULL,
            (SELECT if(7, (SELECT DISTINCT 255 AS alias980, NULL), in(part_date)))
        )), toLowCardinality(materialize(1025))), toNullable(257)), 100),
        NULL, toLowCardinality(materialize('ffffffff-ffff-ffff-ffff-ffffffffffff')),
        (SELECT minus(NULL, '33')))
    )), divide(1025, NULL)), NULL), pk_date),
    isNull(plus(plus((pk_date IN toDate(toNullable(toFixedString('2018-04-19', 10)))),
        modulo(-2147483649, 65535)), toUInt32(toLowCardinality(2147483646)))),
    modulo((SELECT DISTINCT '-0.00\0000'), (SELECT DISTINCT toNullable(NULL)))
), assumeNotNull(-1))) NOT IN (pk_date)
FORMAT Null;

DROP TABLE t_04054;
