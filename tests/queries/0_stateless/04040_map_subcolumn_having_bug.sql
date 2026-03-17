-- Regression test: FunctionToSubcolumnsPass must not rewrite m['key'] -> m.key_<key>
-- in HAVING when the full map m is also read (e.g. GROUP BY m), because the resulting
-- subcolumn would not be in GROUP BY and would cause an exception.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_having;

CREATE TABLE t_map_having (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_having SELECT number, map('key1', number, 'key2', number + 1) FROM numbers(10);
OPTIMIZE TABLE t_map_having FINAL;

-- HAVING with full map in GROUP BY: must not rewrite m['key1'] to subcolumn
SELECT m FROM t_map_having GROUP BY m HAVING m['key1'] > 5 ORDER BY m['key1']
    SETTINGS optimize_functions_to_subcolumns = 1;

-- Verify it matches the unoptimized baseline
SELECT m FROM t_map_having GROUP BY m HAVING m['key1'] > 5 ORDER BY m['key1']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE optimization must still work (regression guard)
SELECT m['key1'] FROM t_map_having WHERE m['key1'] > 5 ORDER BY m['key1']
    SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_having;
