-- Tags: no-random-settings

-- Regression test for Block structure mismatch in UnionStep when using
-- normal projections with parallel replicas.
--
-- When optimizePrewhere moves the WHERE condition to PREWHERE, it may
-- restore columns consumed by the filter as extra outputs via
-- splitAndFillPrewhereInfo. These extra columns propagate through
-- ActionsDAG::mergeInplace into the projection query DAG because later
-- ExpressionSteps do not consume them. When the projection chain replaces
-- the local read plan, these extra columns cause a header mismatch with
-- the remote replica plan in the UnionStep.

SET enable_analyzer = 1;
SET allow_experimental_parallel_reading_from_replicas = 2;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET parallel_replicas_support_projection = 1;
-- Disable lazy materialization so it does not mask the header mismatch
-- (lazy materialization adds its own projection step that strips extra PREWHERE columns).
SET query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS test_projection_pr;

CREATE TABLE test_projection_pr
(
    id UInt64,
    url String,
    region String,
    payload String,
    PROJECTION region_proj
    (
        SELECT url, region, payload ORDER BY region
    )
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 32;

-- Insert enough rows so that column sizes are meaningful and
-- optimizePrewhere definitely moves the WHERE filter to PREWHERE.
INSERT INTO test_projection_pr
    SELECT
        number,
        'https://example.com/page' || toString(number),
        if(number % 3 = 0, 'europe', if(number % 3 = 1, 'us_west', 'asia')),
        randomPrintableASCII(200)
    FROM numbers(10000);

-- Single part so the projection covers all data.
OPTIMIZE TABLE test_projection_pr FINAL;

-- The query selects only `url` and filters on `region`.
-- The projection is ordered by `region`, giving it fewer marks to read.
-- Since `region` is NOT in SELECT, optimizePrewhere restores it as an
-- extra column that persists in the projection query DAG, causing a
-- header mismatch with the remote plan in the parallel replicas UnionStep.
SELECT url FROM test_projection_pr WHERE region = 'europe' ORDER BY url LIMIT 1;

-- Verify the plan-level fix: when all parts come from the projection and
-- PREWHERE adds extra columns, a converting Expression step must be present
-- in the plan to strip those columns before the Union.
-- Without this fix, the step is absent and the Union inputs have mismatched
-- headers (handled at pipeline level by #99515, but incorrect at plan level).
SELECT count() > 0
FROM (
    EXPLAIN PLAN
    SELECT url FROM test_projection_pr WHERE region = 'europe' ORDER BY url LIMIT 1
)
WHERE explain LIKE '%Convert projection output to match expected header%';

DROP TABLE test_projection_pr;
