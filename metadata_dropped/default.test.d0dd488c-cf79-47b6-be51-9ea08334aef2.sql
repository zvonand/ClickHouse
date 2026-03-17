ATTACH TABLE _ UUID 'd0dd488c-cf79-47b6-be51-9ea08334aef2'
(
    `id` UInt64,
    `json` JSON(max_dynamic_paths = 8, `a.b` Array(JSON))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 10000000000, index_granularity = 8192
