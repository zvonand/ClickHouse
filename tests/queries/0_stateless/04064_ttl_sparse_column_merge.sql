-- Regression test: TTL column with sparse serialization caused
-- "Unexpected type of result TTL column" during merge because
-- ColumnSparse was not unwrapped before type dispatch.
-- The TTL expression must reference the column directly (TTL ts, not TTL ts + INTERVAL ...)
-- so that the result column is looked up in the block and returned as-is (sparse).

DROP TABLE IF EXISTS t_ttl_sparse;

CREATE TABLE t_ttl_sparse (key UInt64, ts DateTime)
ENGINE = MergeTree ORDER BY key
TTL ts
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.001,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1,
         min_bytes_for_wide_part = 0;

-- Insert rows where ts is all zeros (defaults) so the column becomes sparse.
INSERT INTO t_ttl_sparse (key) SELECT number FROM numbers(100);
INSERT INTO t_ttl_sparse (key) SELECT number + 100 FROM numbers(100);

OPTIMIZE TABLE t_ttl_sparse FINAL;

SELECT count() FROM t_ttl_sparse;

DROP TABLE t_ttl_sparse;
