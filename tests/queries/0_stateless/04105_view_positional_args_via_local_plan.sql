-- Verify that positional arguments inside views are resolved correctly via the
-- local-plan path (createLocalPlan) triggered when prefer_localhost_replica=1
-- routes a distributed query shard to the local node.
--
-- createLocalPlan sets enable_positional_arguments=false on a copy of the
-- context to prevent double-resolution of the outer query's positional args.
-- This must not prevent the view's own positional args from being resolved,
-- because the view is expanded on the local node (not on the initiator).
--
-- The fix: replaceNodesWithPositionalArguments checks getQueryContext() for
-- the user's original setting rather than the system-overridden copy.
--
-- Refs: https://github.com/ClickHouse/ClickHouse/issues/89940

DROP TABLE IF EXISTS test_table SYNC;
DROP VIEW IF EXISTS test_view SYNC;

CREATE TABLE test_table (str String) ENGINE = MergeTree ORDER BY str;
INSERT INTO test_table VALUES ('a'), ('b'), ('c');

-- GROUP BY 1 should resolve to GROUP BY str → 3 distinct groups.
-- Without the fix, createLocalPlan's enable_positional_arguments=false caused
-- GROUP BY 1 to stay as a literal constant, which was then removed by
-- optimize_group_by_constant_keys, collapsing the query to a global aggregate.
CREATE VIEW test_view AS SELECT str, count() AS cnt FROM test_table GROUP BY 1;

-- 127.0.0.1 is the local host; prefer_localhost_replica=1 guarantees that
-- this single shard is served by createLocalPlan (no remote TCP connection).
SELECT '--- prefer_localhost_replica=1, positional args enabled: 3 ---';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), test_view)
SETTINGS prefer_localhost_replica = 1;

-- Sanity check: disabling positional arguments must also be respected on
-- the local-plan path (GROUP BY 1 stays literal → NOT_AN_AGGREGATE).
SELECT '--- prefer_localhost_replica=1, positional args disabled: error ---';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), test_view)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 0; -- { serverError 215 }

DROP TABLE test_table SYNC;
DROP VIEW test_view SYNC;
