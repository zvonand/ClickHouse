-- Tags: zookeeper

-- Test that parallel replicas work with a simple view over a MergeTree table.
-- Simplified reproduction of a customer schema: a SummingMergeTree table
-- with a view defined as SELECT * FROM that table.

DROP TABLE IF EXISTS t_rtb_hourly SYNC;
DROP VIEW IF EXISTS v_rtb_hourly;

CREATE TABLE t_rtb_hourly
(
    Hour DateTime('UTC'),
    NetworkId UInt16,
    BundleDomain Int32,
    AppSiteChannelId Int32,
    DeviceTypeId UInt16,
    Impressions UInt64,
    Clicks UInt64,
    PriceEuro Decimal(18, 6)
)
ENGINE = MergeTree() --ReplicatedMergeTree('/clickhouse/tables/{database}/04057_t_rtb_hourly', '{replica}')
PARTITION BY toYearWeek(Hour)
ORDER BY (Hour, NetworkId, BundleDomain, AppSiteChannelId, DeviceTypeId);

INSERT INTO t_rtb_hourly
    SELECT
        toDateTime('2024-01-01', 'UTC') + toIntervalHour(number % 720),
        number % 3,
        number % 20,
        number % 20,
        number % 5,
        1,
        1,
        1.0
    FROM numbers(10000);

CREATE VIEW v_rtb_hourly AS SELECT * FROM t_rtb_hourly;

SET automatic_parallel_replicas_mode = 0;

-- Verify the view returns correct results without parallel replicas
SELECT 'non-parallel';
SELECT
    BundleDomain as AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_rtb_hourly
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL;

-- Same query with parallel replicas — results must match
SELECT 'parallel';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_rtb_hourly
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1;

-- Same query with parallel replicas with allowed view over MT table
SELECT 'parallel, allowed view over mt';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_rtb_hourly
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_rtb_hourly;
DROP TABLE t_rtb_hourly SYNC;
