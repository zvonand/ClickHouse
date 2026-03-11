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

SET mutations_sync = 1;
ALTER TABLE t_ephemeral_materialized UPDATE c3 = false WHERE c2 = 'Hello World!';

SELECT c2, c3 FROM t_ephemeral_materialized;

ALTER TABLE t_ephemeral_materialized DELETE WHERE c2 = 'Hello World!';

SELECT count() FROM t_ephemeral_materialized;

DROP TABLE t_ephemeral_materialized;

-- Mixed case: table with BOTH ephemeral-dependent AND non-ephemeral MATERIALIZED columns.
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

SET mutations_sync = 1;
ALTER TABLE t_mixed_materialized UPDATE c3 = 'world' WHERE c2 = 'Hello World!';

SELECT c2, c3, c4 FROM t_mixed_materialized;

DROP TABLE t_mixed_materialized;
