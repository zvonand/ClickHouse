-- Regression test: constant folded from UNION (INTERSECT ALL) node should not
-- cause "Invalid action query tree node" exception in `calculateActionNodeName`.
-- The `remote` call forces SECONDARY_QUERY context where `isASTLevelOptimizationAllowed` returns false,
-- which is required to reach the affected code path.
-- The INTERSECT ALL expression must be in the SELECT list (not in WHERE as a scalar subquery)
-- so that it is sent as part of the query tree to the remote node with the UNION source expression preserved.
SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10));
SELECT (SELECT 1 INTERSECT ALL SELECT 1) FROM remote('127.0.0.1', numbers(1));
SELECT (SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10))) FROM remote('127.0.0.1', numbers(1));
