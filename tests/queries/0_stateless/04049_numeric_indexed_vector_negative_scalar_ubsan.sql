-- Negative scalar values passed to NumericIndexedVector pointwise operations
-- should throw INCORRECT_DATA, not trigger undefined behavior.

DROP TABLE IF EXISTS t_int;
CREATE TABLE t_int (ds Date, uin UInt32, value Int64) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t_int VALUES ('2023-12-26', 1, 1);

SELECT numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(uin, value), -1) FROM t_int; -- { serverError INCORRECT_DATA }

DROP TABLE t_int;

DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (ds Date, uin UInt32, value Float64) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t_float VALUES ('2023-12-26', 1, 1.5);

SELECT numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(uin, value), -1.0) FROM t_float; -- { serverError INCORRECT_DATA }

DROP TABLE t_float;
