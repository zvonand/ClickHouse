-- Tags: no-fasttest

-- Verify that `from_env` substitutions in dynamic disk configuration are blocked by default
-- (server setting `dynamic_disk_allow_from_env` defaults to false).

DROP TABLE IF EXISTS test;

CREATE TABLE test (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    name = 'test_from_env',
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'from_env S3_ENDPOINT',
    access_key_id = clickhouse,
    secret_access_key = clickhouse); -- { serverError ACCESS_DENIED }

DROP TABLE IF EXISTS test;
