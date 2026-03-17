-- Tags: no-fasttest
-- Tag no-fasttest: requires s3 storage

-- Test that object storage listing/reading ProfileEvents are populated

-- Write two separate files so we can verify listing counts
INSERT INTO FUNCTION s3('http://localhost:11111/test/{database}/04042_data/a.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_upload=1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/{database}/04042_data/b.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_upload=1;

-- Read with glob pattern
SELECT count() FROM s3('http://localhost:11111/test/{database}/04042_data/*.csv', 'test', 'testtest', 'CSV', 'x UInt64') SETTINGS log_queries=1;

SYSTEM FLUSH LOGS query_log;

-- Verify listing and reading events are present.
-- ObjectStorageGlobFilteredObjects and ObjectStoragePredicateFilteredObjects count removed objects,
-- which should be 0 here since all listed files match the glob and no predicate filter is applied.
SELECT
    ProfileEvents['ObjectStorageListedObjects'] AS has_listed,
    ProfileEvents['ObjectStorageGlobFilteredObjects'] AS glob_filtered,
    ProfileEvents['ObjectStoragePredicateFilteredObjects'] AS predicate_filtered,
    ProfileEvents['ObjectStorageReadObjects'] AS has_read
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%SELECT count() FROM s3%04042_data/*.csv%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;


INSERT INTO FUNCTION s3('http://localhost:11111/test/{database}/04042_data/a_a.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_upload=1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/{database}/04042_data/a_b.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_upload=1;

-- Read with glob pattern
SELECT count() FROM s3('http://localhost:11111/test/{database}/04042_data/a_*.csv', 'test', 'testtest', 'CSV', 'x UInt64') SETTINGS log_queries=1;

SYSTEM FLUSH LOGS query_log;

-- Verify listing and reading events are present.
-- ObjectStorageGlobFilteredObjects and ObjectStoragePredicateFilteredObjects count removed objects,
-- which should be 0 here since all listed files match the glob and no predicate filter is applied.
SELECT
    ProfileEvents['ObjectStorageListedObjects'] AS has_listed,
    ProfileEvents['ObjectStorageGlobFilteredObjects'] AS glob_filtered,
    ProfileEvents['ObjectStoragePredicateFilteredObjects'] AS predicate_filtered,
    ProfileEvents['ObjectStorageReadObjects'] AS has_read
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%SELECT count() FROM s3%04042_data/a_*.csv%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;
