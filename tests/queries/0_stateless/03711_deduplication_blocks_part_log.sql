-- Tags: long, no-sanitizers, no-parallel, no-parallel-replicas, no-async-insert

-- no-parallel-replicas -- https://github.com/ClickHouse/ClickHouse/issues/90063

-- Tags: deduplication blocks have different values for sync and async inserts,
-- async insert calculates it as a has of data in the block,
-- sync insert uses MergeTreePartWriter's hash which covers only data in the partition.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with VALUES (1, 'a1'), (1, 'b1'), (1, 'c1');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with VALUES (2, 'a2'), (2, 'b2'), (2, 'c2');

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.03711_table;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.03711_table
(
    id UInt32
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.03711_table;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_1
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_1;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_2
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_2;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_1;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_1
TO {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_1 AS
SELECT r.id as id, r.value as value FROM {CLICKHOUSE_DATABASE_1:Identifier}.03711_table as l JOIN {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with as r ON l.id == r.id and l.id = 1;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_2;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_2
TO {CLICKHOUSE_DATABASE_1:Identifier}.03711_mv_table_2 AS
SELECT r.id as id, r.value as value FROM {CLICKHOUSE_DATABASE_1:Identifier}.03711_table as l JOIN {CLICKHOUSE_DATABASE_1:Identifier}.03711_join_with as r ON l.id == r.id and l.id = 2;

SET deduplicate_blocks_in_dependent_materialized_views=1;

SET max_block_size=1;
SET max_insert_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.03711_table VALUES (1), (2);

SYSTEM FLUSH LOGS part_log;

SELECT table, name, argMax(part_type, event_time_microseconds), argMax(deduplication_block_ids, event_time_microseconds) FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    table IN ['03711_join_with', '03711_table', '03711_mv_table_1', '03711_mv_table_2']
    AND database = '{CLICKHOUSE_DATABASE_1}'
group BY database, table, name
ORDER BY ALL;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
