DROP TABLE IF EXISTS alter_modify_column_ttl_without_type;

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE alter_modify_column_ttl_without_type
(
    uid Int16,
    name String,
    age Date
)
ENGINE = MergeTree
ORDER BY uid;

ALTER TABLE alter_modify_column_ttl_without_type MODIFY COLUMN name TTL age + INTERVAL 1 DAY;
SHOW CREATE TABLE alter_modify_column_ttl_without_type FORMAT TSVRaw;

DROP TABLE alter_modify_column_ttl_without_type;
