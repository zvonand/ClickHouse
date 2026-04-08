-- Regression test: SIGSEGV in executeWithoutLowCardinalityColumns during
-- nested view filter propagation when join_use_nulls changes column types
-- between view creation (metadata) and query time (actual plan header).
--
-- The outer WHERE filter DAG is compiled against the view's metadata types
-- (non-Nullable), but the inner LEFT JOIN produces Nullable columns when
-- join_use_nulls=1. Without a type check, the filter's pre-compiled function
-- objects receive columns of the wrong type, causing a wild-pointer crash.

SET join_use_nulls = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1_04092;
DROP TABLE IF EXISTS t2_04092;
DROP VIEW IF EXISTS v_t2_04092;
DROP VIEW IF EXISTS v_join_04092;

CREATE TABLE t1_04092 (id UInt32, a UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2_04092 (id UInt32, b UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1_04092 VALUES (1, 100), (2, 200);
INSERT INTO t2_04092 VALUES (1, 10);

-- Wrap t2 in a view so the join's inner query contains a StorageView,
-- which sets collect_filters = true in collectFiltersForAnalysis.
CREATE VIEW v_t2_04092 AS SELECT * FROM t2_04092;

-- View with LEFT JOIN, created with join_use_nulls=0.
-- Metadata types: id UInt32, a UInt32, b UInt32 (non-Nullable).
CREATE VIEW v_join_04092 AS
    SELECT t1_04092.id AS id, t1_04092.a AS a, v_t2_04092.b AS b
    FROM t1_04092 LEFT JOIN v_t2_04092 ON t1_04092.id = v_t2_04092.id;

-- Now query with join_use_nulls=1.  The inner LEFT JOIN produces
-- b as Nullable(UInt32), but the propagated filter expects UInt32.
-- Before the fix this was a SIGSEGV.
SET join_use_nulls = 1;
SELECT * FROM v_join_04092 WHERE b + 0 IN (10) ORDER BY id;

DROP VIEW v_join_04092;
DROP VIEW v_t2_04092;
DROP TABLE t2_04092;
DROP TABLE t1_04092;
