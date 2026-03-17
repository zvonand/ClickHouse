-- Tags: no-fasttest
-- Positive-path test for PR #99306: `Buffer` over a `MergeTree` table that has
-- a `SAMPLE BY` clause. `StorageBuffer::supportsSampling` was changed to
-- delegate to the destination table, so `SAMPLE` must succeed when the
-- destination supports sampling.

DROP TABLE IF EXISTS t_04042_mt;
DROP TABLE IF EXISTS t_04042_buf;

CREATE TABLE t_04042_mt (x UInt64, y String) ENGINE = MergeTree ORDER BY x SAMPLE BY x;
CREATE TABLE t_04042_buf (x UInt64, y String)
    ENGINE = Buffer(currentDatabase(), t_04042_mt, 1, 0, 0, 1, 1, 1, 1);

INSERT INTO t_04042_mt SELECT number, toString(number) FROM numbers(10000);

-- SAMPLE on a Buffer backed by a table with SAMPLE BY must succeed
SELECT count() > 0 FROM t_04042_buf SAMPLE 0.5;
SELECT count() > 0 FROM t_04042_buf SAMPLE 0.5 SETTINGS enable_analyzer = 0;

DROP TABLE t_04042_buf;
DROP TABLE t_04042_mt;
