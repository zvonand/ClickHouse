-- Regression test: constant folded from UNION (INTERSECT ALL) node should not
-- cause "Invalid action query tree node" exception in `calculateActionNodeName`.
-- The bug requires a constant with UNION source expression in a context where
-- `isASTLevelOptimizationAllowed` returns false (SECONDARY_QUERY or ignoreASTOptimizations).
-- The original bug was found by the AST fuzzer in stress test (TSan).
-- Note: scalar subqueries are pre-evaluated by the initiator and serialized as _CAST,
-- so the UNION source expression is not preserved when sent to remote nodes.
-- These queries serve as regression tests for the fix.
SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10));
SELECT (SELECT 1 INTERSECT ALL SELECT 1) FROM remote('127.0.0.1', numbers(1));
SELECT (SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10))) FROM remote('127.0.0.1', numbers(1));
