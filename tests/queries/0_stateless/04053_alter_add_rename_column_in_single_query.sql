-- Regression test: ALTER TABLE with ADD COLUMN followed by RENAME COLUMN in the
-- same statement caused "Cannot find column" exception because ADD COLUMN was not
-- applied to intermediate metadata when it didn't require a mutation stage.
-- https://github.com/ClickHouse/ClickHouse/issues/100328

CREATE TABLE t_100328 (v1 UInt32, v2 String, v3 Date) ENGINE = MergeTree() ORDER BY v1;
INSERT INTO t_100328 VALUES (1, 'hello', '2024-01-01');

ALTER TABLE t_100328 ADD COLUMN v4 String DEFAULT 'new', RENAME COLUMN v4 TO v5;
SELECT v1, v5 FROM t_100328;

ALTER TABLE t_100328 ADD COLUMN v6 Int32 DEFAULT 42, MODIFY COLUMN v2 LowCardinality(String), DROP COLUMN v3, RENAME COLUMN v6 TO v7;
SELECT v1, v2, v5, v7 FROM t_100328;

DROP TABLE t_100328;
