-- Regression test: correlated subquery plans may include steps (e.g. SortingStep)
-- that do not carry their own ActionsDAG. Before the fix, the base
-- IQueryPlanStep::hasCorrelatedExpressions threw NOT_IMPLEMENTED for such steps,
-- crashing decorrelation even when no PLACEHOLDER nodes were present in the step.
-- The fix changes the default to return false.
--
-- Additionally, the fix adds guards in FutureSetFromSubquery::buildSetInplace and
-- buildOrderedSetInplace to detect PLACEHOLDER nodes before attempting to execute
-- correlated subquery plans inline. That path is only reachable through the AST
-- fuzzer (the analyzer rejects correlated IN subqueries before reaching the planner)
-- and therefore cannot be tested directly from SQL.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, b UInt32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t2 (x UInt32, y UInt32) ENGINE = MergeTree() ORDER BY x;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (10, 100), (20, 200), (40, 400);

-- Correlated scalar subquery whose plan includes a SortingStep (from ORDER BY x).
-- On master this throws NOT_IMPLEMENTED because SortingStep::hasCorrelatedExpressions
-- uses the base implementation which throws. After the fix it returns false and
-- decorrelation succeeds.
SELECT a, (SELECT x FROM t2 WHERE t2.y = t1.a * 100 ORDER BY x LIMIT 1) as s
FROM t1
ORDER BY a;

DROP TABLE t1;
DROP TABLE t2;
