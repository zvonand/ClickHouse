-- Regression test: evaluatePartialResult during convertAnyJoinToSemiOrAntiJoin
-- must not throw LOGICAL_ERROR when an IN function references a not-ready Set.
-- https://github.com/ClickHouse/ClickHouse/pull/99939

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400);

-- ANY LEFT JOIN with a filter that uses IN with a subquery.
-- The optimizer tries convertAnyJoinToSemiOrAntiJoin and calls evaluatePartialResult
-- which does a dry run of the IN function before the subquery set is ready.
SELECT a, b, c
FROM t1
ANY LEFT JOIN t2 USING (a)
WHERE c IN (SELECT c FROM t2 WHERE c > 0)
ORDER BY a;

DROP TABLE t1;
DROP TABLE t2;
