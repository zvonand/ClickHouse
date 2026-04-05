-- Verify that the 'alterable' column description in system.s3_queue_settings
-- and system.azure_queue_settings correctly states that 1 = can change (not 0 = can change).
-- Both tables share the same StorageSystemObjectStorageQueueSettings template.
-- See: https://github.com/ClickHouse/ClickHouse/issues/101694

SELECT table,
       comment LIKE '%0 — Current user can''t change the setting%' AS zero_means_not_changeable,
       comment LIKE '%1 — Current user can change the setting%' AS one_means_changeable
FROM system.columns
WHERE database = 'system'
  AND table IN ('s3_queue_settings', 'azure_queue_settings')
  AND name = 'alterable'
ORDER BY table;
