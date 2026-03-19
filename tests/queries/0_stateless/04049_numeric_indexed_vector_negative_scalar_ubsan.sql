DROP TABLE IF EXISTS t;
CREATE TABLE t (ds Date, uin UInt32, value Int64) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t VALUES ('2023-12-26', 1, 1);

SELECT numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(uin, value), -1) FROM t; -- { serverError INCORRECT_DATA }

DROP TABLE t;
