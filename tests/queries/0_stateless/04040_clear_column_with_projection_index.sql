-- Tags: no-random-merge-tree-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99388
-- CLEAR COLUMN must rebuild projections that depend on the cleared column,
-- otherwise stale projection data causes sort order violations during merge.

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
