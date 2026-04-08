-- Regression test for convertToFullColumnIfSparse in executeExpressionAndGetColumn.
--
-- When a TTL expression directly references a column (e.g. TTL ts), and that column
-- uses sparse serialization, executeExpressionAndGetColumn returns it as ColumnSparse.
-- The fix converts it to a dense column at the source, so no downstream TTL algorithm
-- needs to handle ColumnSparse individually.
--
-- This test exercises three TTL algorithm paths with sparse DateTime columns:
--  1. Row TTL delete   (TTLDeleteAlgorithm)
--  2. Column TTL       (TTLColumnAlgorithm)
--  3. GROUP BY TTL     (TTLAggregationAlgorithm)

-- Test 1: Row TTL with sparse DateTime column
DROP TABLE IF EXISTS t_ttl_sparse_row;

CREATE TABLE t_ttl_sparse_row
(
    id UInt64,
    ts DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL ts
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_row;

-- All ts=toDateTime(0): 100% defaults -> column stored as ColumnSparse.
-- table_ttl.min=0 -> force_ttl=true on merge, so TTL transform runs.
INSERT INTO t_ttl_sparse_row SELECT number, toDateTime(0) FROM numbers(100);
INSERT INTO t_ttl_sparse_row SELECT number + 100, toDateTime(0) FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_row;
OPTIMIZE TABLE t_ttl_sparse_row FINAL;

-- isTTLExpired(0)=false (0 is "no expiry" sentinel) -> all 200 rows survive.
SELECT count() FROM t_ttl_sparse_row;

DROP TABLE t_ttl_sparse_row;


-- Test 2: Column TTL with sparse DateTime column
DROP TABLE IF EXISTS t_ttl_sparse_col;

CREATE TABLE t_ttl_sparse_col
(
    id UInt64,
    ts DateTime,
    val String TTL ts
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_col;

INSERT INTO t_ttl_sparse_col SELECT number, toDateTime(0), 'hello' FROM numbers(100);
INSERT INTO t_ttl_sparse_col SELECT number + 100, toDateTime(0), 'world' FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_col;
OPTIMIZE TABLE t_ttl_sparse_col FINAL;

-- ts=0 -> column TTL not applied -> val preserved for all rows.
SELECT count(), countIf(val != '') FROM t_ttl_sparse_col;

DROP TABLE t_ttl_sparse_col;


-- Test 3: GROUP BY TTL with sparse DateTime column
DROP TABLE IF EXISTS t_ttl_sparse_agg;

CREATE TABLE t_ttl_sparse_agg
(
    id UInt64,
    ts DateTime,
    val UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL ts GROUP BY id SET val = max(val)
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_agg;

INSERT INTO t_ttl_sparse_agg SELECT number, toDateTime(0), number FROM numbers(100);
INSERT INTO t_ttl_sparse_agg SELECT number + 100, toDateTime(0), number + 100 FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_agg;
OPTIMIZE TABLE t_ttl_sparse_agg FINAL;

-- ts=0 -> no aggregation triggered -> all 200 rows survive.
SELECT count() FROM t_ttl_sparse_agg;

DROP TABLE t_ttl_sparse_agg;
