-- Tags: no-fasttest

SET mutations_sync = 1;

-- Case 1: MATERIALIZED depends only on EPHEMERAL — UPDATE and DELETE should work
DROP TABLE IF EXISTS t_ephemeral_materialized;

CREATE TABLE t_ephemeral_materialized
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED tryBase64Decode(c1),
    c3 Bool
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_ephemeral_materialized (c1, c3) VALUES ('SGVsbG8gV29ybGQh', true);

SELECT c2, c3 FROM t_ephemeral_materialized;

ALTER TABLE t_ephemeral_materialized UPDATE c3 = false WHERE c2 = 'Hello World!';
SELECT c2, c3 FROM t_ephemeral_materialized;

ALTER TABLE t_ephemeral_materialized DELETE WHERE c2 = 'Hello World!';
SELECT count() FROM t_ephemeral_materialized;

DROP TABLE t_ephemeral_materialized;

-- Case 2: Table with BOTH ephemeral-dependent AND non-ephemeral MATERIALIZED columns.
-- Updating the source of the non-ephemeral one should recalculate it without
-- trying to recalculate the ephemeral-dependent one.
DROP TABLE IF EXISTS t_mixed_materialized;

CREATE TABLE t_mixed_materialized
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED tryBase64Decode(c1),
    c3 String,
    c4 String MATERIALIZED upper(c3)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_mixed_materialized (c1, c3) VALUES ('SGVsbG8gV29ybGQh', 'hello');
SELECT c2, c3, c4 FROM t_mixed_materialized;

ALTER TABLE t_mixed_materialized UPDATE c3 = 'world' WHERE c2 = 'Hello World!';
SELECT c2, c3, c4 FROM t_mixed_materialized;

DROP TABLE t_mixed_materialized;

-- Case 3: Mixed-dependency MATERIALIZED column (EPHEMERAL + ordinary in same expression).
-- Updating the ordinary column succeeds but the MATERIALIZED value stays stale (by design).
DROP TABLE IF EXISTS t_mixed_dep;

CREATE TABLE t_mixed_dep
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED concat(tryBase64Decode(c1), '-', c3),
    c3 String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_mixed_dep (c1, c3) VALUES ('SGVsbG8gV29ybGQh', 'hello');
SELECT c2, c3 FROM t_mixed_dep;

ALTER TABLE t_mixed_dep UPDATE c3 = 'world' WHERE c3 = 'hello';
-- c2 stays stale (Hello World!-hello) because it depends on EPHEMERAL c1 which cannot be re-read
SELECT c2, c3 FROM t_mixed_dep;

DROP TABLE t_mixed_dep;

-- Case 4: Lightweight delete path
DROP TABLE IF EXISTS t_ephemeral_lightweight;

CREATE TABLE t_ephemeral_lightweight
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED tryBase64Decode(c1),
    c3 UInt64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_ephemeral_lightweight (c1, c3) VALUES ('SGVsbG8gV29ybGQh', 1);
INSERT INTO t_ephemeral_lightweight (c1, c3) VALUES ('SGVsbG8gV29ybGQh', 2);

SELECT c2, c3 FROM t_ephemeral_lightweight ORDER BY c3;

DELETE FROM t_ephemeral_lightweight WHERE c3 = 1;
SELECT c2, c3 FROM t_ephemeral_lightweight ORDER BY c3;

DROP TABLE t_ephemeral_lightweight;
