-- Tags: no-random-merge-tree-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99388
-- CLEAR COLUMN must rebuild projections that depend on the cleared column,
-- otherwise stale projection data causes sort order violations during merge.

SET mutations_sync = 2;

-- Test compact parts
DROP TABLE IF EXISTS test_compact;

CREATE TABLE test_compact
(
    c0 Int,
    c1 Int,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';

INSERT INTO test_compact SELECT number, number FROM numbers(10);
ALTER TABLE test_compact CLEAR COLUMN c1;

-- After clearing c1, all values should be 0 (default), so count where c1 == 0 should be 10.
SELECT count() FROM test_compact WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT count() FROM test_compact WHERE c1 = 0 SETTINGS optimize_use_projections = 0;

DROP TABLE test_compact;

-- Test wide parts
DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_wide
(
    c0 Int,
    c1 Int,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_wide SELECT number, number FROM numbers(10);
ALTER TABLE test_wide CLEAR COLUMN c1;

-- After clearing c1, all values should be 0 (default), so count where c1 == 0 should be 10.
SELECT count() FROM test_wide WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT count() FROM test_wide WHERE c1 = 0 SETTINGS optimize_use_projections = 0;

DROP TABLE test_wide;

-- Test with Array type and projection index (original reproducer from the issue)
DROP TABLE IF EXISTS t0;

CREATE TABLE t0
(
    c0 Int,
    c1 Array(Nullable(String)),
    PROJECTION p0 INDEX c1 TYPE basic
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO TABLE t0 (c0, c1) SELECT
    CAST(number AS Int),
    [CAST((-number) % 2 AS String), CAST(number % 23 AS String), NULL, 'watch',
     CAST(number % 13 AS String), NULL,
     if(number % 10, CAST((-number) % 24 AS String), CAST(number AS String)),
     if(number % 14, CAST(number AS String), CAST(number % 4 AS String)),
     CAST((-number) AS String), CAST(number AS String)]
FROM numbers(100);

INSERT INTO TABLE t0 (c1, c0) SELECT c1, c0
FROM generateRandom('c1 Array(Nullable(String)), c0 Int', 5005534133878302057, 69, 10) LIMIT 100;

INSERT INTO TABLE t0 (c0, c1) SELECT
    CAST(number AS Int),
    [NULL, CAST(number AS String), 'said',
     if(number % 30, CAST(number % 21 AS String), CAST(number AS String)),
     'found',
     if(number % 7, CAST(number AS String), CAST((-number) % 20 AS String)),
     if(number % 10, CAST((-number) AS String), CAST(number AS String)),
     NULL, CAST(number AS String),
     if(number % 29, CAST(number AS String), CAST(number % 11 AS String))]
FROM numbers(100);

INSERT INTO TABLE t0 (c1, c0) SELECT c1, c0
FROM generateRandom('c1 Array(Nullable(String)), c0 Int', 13205654012336841338, 37, 10) LIMIT 100;

INSERT INTO TABLE t0 (c0, c1) SELECT c0, c1
FROM generateRandom('c0 Int, c1 Array(Nullable(String))', 3213559484157746812, 67, 10) LIMIT 100;

ALTER TABLE t0 CLEAR COLUMN c1;

INSERT INTO TABLE t0 (c0, c1) SELECT c0, c1
FROM generateRandom('c0 Int, c1 Array(Nullable(String))', 2462998867452601120, 64, 10) LIMIT 100;

-- Verify projection exists on parts after CLEAR COLUMN (rebuilt, not dropped).
SELECT count() > 0 FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't0' AND active AND name = 'p0';

-- Read c1 before merge to verify per-part projection data is consistent.
SELECT c1 FROM t0 ORDER BY c1 FORMAT Null;

-- Force merge to trigger projection merge with mixed data.
OPTIMIZE TABLE t0 FINAL;

-- Read c1 after merge to verify merged projection data is consistent.
SELECT c1 FROM t0 ORDER BY c1 FORMAT Null;

-- Verify projection still exists after merge (rebuilt, not silently dropped).
SELECT count() > 0 FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't0' AND active AND name = 'p0';

SELECT count() FROM t0;

DROP TABLE t0;
