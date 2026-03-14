-- Tags: no-random-settings

-- Regression test for Block structure mismatch in UnionStep when using
-- normal projections with parallel replicas (the projection optimization
-- could produce extra columns that didn't match the remote replica header).

SET enable_analyzer = 1;
SET allow_experimental_parallel_reading_from_replicas = 2;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET min_table_rows_to_use_projection_index = 1000000;
SET parallel_replicas_support_projection = 1;

DROP TABLE IF EXISTS test_projection_pr;

CREATE TABLE test_projection_pr
(
    id UInt64,
    event_date Date,
    user_id UInt32,
    url String,
    region String,
    PROJECTION region_url_proj
    (
        SELECT _part_offset ORDER BY region, url
    )
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS
    index_granularity = 1, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO test_projection_pr VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe');
INSERT INTO test_projection_pr VALUES (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west');

OPTIMIZE TABLE test_projection_pr FINAL;

SELECT url FROM test_projection_pr WHERE region = 'europe' ORDER BY ALL;

DROP TABLE test_projection_pr;
