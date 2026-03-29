-- Tags: no-fasttest
-- Tag no-fasttest: needs s3

-- Verify that storage_configuration disk settings properly override global <s3>
-- endpoint configuration for the DiskS3 path (S3ObjectStorage::applyNewSettings).
--
-- The test depends on:
-- - tests/config/config.d/storage_conf_04068.xml which defines s3_disk_04068
--   with max_single_part_upload_size = 10000 (small, to force multipart upload).
-- - tests/config/config.d/s3_settings_override.xml which configures a matching
--   global <s3> endpoint with max_single_part_upload_size = 100Mi.
--
-- Without the fix, the global endpoint config would override the disk config
-- setting, resulting in a single-part upload instead of multipart.

DROP TABLE IF EXISTS t_04068_s3_disk_override;

CREATE TABLE t_04068_s3_disk_override (number UInt64)
ENGINE = MergeTree ORDER BY number
SETTINGS storage_policy = 's3_04068';

INSERT INTO t_04068_s3_disk_override SELECT number FROM numbers(1000000);

SYSTEM FLUSH LOGS query_log;

-- With max_single_part_upload_size = 10000 from disk config, the data should
-- be uploaded via multipart (CreateMultipartUpload + UploadPart + CompleteMultipartUpload).
-- If the global endpoint config (100Mi) had incorrectly taken priority, it would be a single PutObject.
SELECT
    ProfileEvents['S3CreateMultipartUpload'] >= 1 AS has_multipart_create,
    ProfileEvents['S3UploadPart'] >= 1 AS has_upload_parts,
    ProfileEvents['S3CompleteMultipartUpload'] >= 1 AS has_multipart_complete
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%t_04068_s3_disk_override%'
    AND query LIKE '%INSERT%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE t_04068_s3_disk_override;
