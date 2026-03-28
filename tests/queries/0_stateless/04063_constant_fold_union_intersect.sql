-- Regression test: constant folded from UNION (INTERSECT ALL) node should not
-- cause "Invalid action query tree node" exception in `calculateActionNodeName`.
-- The `remote` call forces SECONDARY_QUERY context where `isASTLevelOptimizationAllowed` returns false,
-- which is required to reach the affected code path.
SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10));
SELECT number FROM remote('127.0.0.1', numbers(1)) WHERE number >= (SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10)));
