-- Regression test: constant folded from UNION (INTERSECT ALL) node should not
-- cause "Invalid action query tree node" exception in `calculateActionNodeName`.
-- The bug requires a constant with UNION source expression in a context where
-- `isASTLevelOptimizationAllowed` returns false (SECONDARY_QUERY context).
-- The original bug was found by the AST fuzzer in stress test (TSan).

-- Basic regression tests (these run on the initiator where AST optimizations are allowed):
SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10));
SELECT (SELECT 1 INTERSECT ALL SELECT 1) FROM remote('127.0.0.1', numbers(1));
SELECT (SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10))) FROM remote('127.0.0.1', numbers(1));

-- The actual bug reproducer: using a view forces the scalar subquery evaluation
-- to happen on the shard (SECONDARY_QUERY context), where `isASTLevelOptimizationAllowed`
-- returns false and the constant retains its UNION source expression.
-- Without the fix, this triggers "Invalid action query tree node" exception.
DROP VIEW IF EXISTS test_04064_v;
CREATE VIEW test_04064_v AS SELECT (SELECT 1 INTERSECT ALL SELECT 1) AS x;
SELECT * FROM remote('127.0.0.1', currentDatabase(), test_04064_v);
DROP VIEW test_04064_v;
