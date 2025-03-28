CREATE TABLE data_horizontal (
    key Int
)
Engine=MergeTree()
ORDER BY key
SETTINGS old_parts_lifetime = 600, vertical_merge_algorithm_min_rows_to_activate = 100000000;

INSERT INTO data_horizontal VALUES (1);
OPTIMIZE TABLE data_horizontal FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT table, part_name, event_type, merge_algorithm FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 'data_horizontal' AND event_type IN ('NewPart', 'MergeParts') ORDER BY event_time_microseconds;

CREATE TABLE data_vertical
(
    key UInt64,
    value String
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity_bytes = 0, enable_mixed_granularity_parts = 0, min_bytes_for_wide_part = 0,
vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
old_parts_lifetime = 600;

INSERT INTO data_vertical VALUES (1, '1');
INSERT INTO data_vertical VALUES (2, '2');
OPTIMIZE TABLE data_vertical FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT table, part_name, event_type, merge_algorithm FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 'data_vertical' AND event_type IN ('NewPart', 'MergeParts') ORDER BY event_time_microseconds;
