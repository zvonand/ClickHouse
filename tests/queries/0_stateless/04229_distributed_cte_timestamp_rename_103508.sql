-- Regression test for #103508:
-- `NOT_FOUND_COLUMN_IN_BLOCK` — the new analyzer renamed `timestamp` to
-- `timestamp_0` inside a CTE wrapping a `Distributed` table when the
-- underlying local table was non-empty, breaking the outer `SELECT`.
-- The bug requires at least one row in the local table to surface; with
-- an empty table the planner short-circuits and no rename happens.

DROP TABLE IF EXISTS dist_t;
DROP TABLE IF EXISTS local_t;

CREATE TABLE local_t (timestamp DateTime64(9), id String) ENGINE = MergeTree ORDER BY timestamp;
CREATE TABLE dist_t AS local_t ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_t, rand());

INSERT INTO local_t VALUES ('2024-01-01', 'a');

WITH A AS
(
    SELECT * FROM dist_t
    WHERE timestamp >= toDateTime64('2023-01-01', 9)
      AND timestamp <  toDateTime64('2025-01-01', 9)
)
SELECT timestamp, id
FROM A
ORDER BY timestamp AS `timestamp` DESC
LIMIT 10
SETTINGS distributed_product_mode = 'allow';

DROP TABLE dist_t;
DROP TABLE local_t;
