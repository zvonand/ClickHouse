-- Tags: no-fasttest

-- Verify that `from_env`, `include`, and `from_zk` in dynamic disk configuration
-- are blocked by default (query settings default to false).

DROP TABLE IF EXISTS test;

-- from_env is blocked by default
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    name = 'test_from_env',
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_env S3_ENDPOINT',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

-- include is blocked by default
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = s3,
    include = 'some_include',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

-- from_zk is blocked by default
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_zk /some/zk/path',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

-- When setting is enabled, from_env is allowed (fails for a different reason - env var resolution or bad config)
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS dynamic_disk_allow_from_env = 1, disk = disk(
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_env HOME',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError BAD_ARGUMENTS }

-- When setting is enabled, include is allowed (fails for a different reason - bad include)
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS dynamic_disk_allow_include = 1, disk = disk(
    type = object_storage,
    object_storage_type = s3,
    include = 'nonexistent_include',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError POCO_EXCEPTION }

-- When setting is enabled, from_zk is allowed (fails for a different reason - no ZK or bad path)
CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS dynamic_disk_allow_from_zk = 1, disk = disk(
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_zk /some/zk/path',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError POCO_EXCEPTION }

DROP TABLE IF EXISTS test;
