-- Regression test: correlated subquery in IN clause should not cause
-- "Trying to execute PLACEHOLDER action" exception.
-- When buildSetInplace encounters a correlated subquery (containing PLACEHOLDER
-- action nodes), it must skip in-place set building and let decorrelation handle it.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, b UInt32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t2 (x UInt32, y UInt32) ENGINE = MergeTree() ORDER BY x;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (10, 100), (20, 200), (40, 400);

-- Correlated subquery in IN clause: the subquery references t1.b from the outer query,
-- which produces PLACEHOLDER action nodes in the subquery plan.
SELECT a, b
FROM t1
WHERE b IN (SELECT x FROM t2 WHERE t2.y = t1.a * 100)
ORDER BY a;

DROP TABLE t1;
DROP TABLE t2;
