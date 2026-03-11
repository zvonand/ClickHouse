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
