-- Regression test for PR #103084: `ColumnQBit::structureEquals` used to always
-- return false for two `ColumnQBit` columns, so `writeSlice` raised a logical
-- error when `if`/`ifNull` ran over tuples or maps containing a `QBit` element.

DROP TABLE IF EXISTS t_qbit_ws;
CREATE TABLE t_qbit_ws (c0 Tuple(Map(String, Nullable(QBit(Float64, 4))))) ENGINE = Memory;
INSERT INTO t_qbit_ws SELECT NULL;
SELECT * FROM t_qbit_ws;
DROP TABLE t_qbit_ws;

DROP TABLE IF EXISTS t_qbit_ws_full;
CREATE TABLE t_qbit_ws_full (
    c0 Tuple(Nullable(Int8), Enum16('a' = 1, 'b' = 2), Map(MultiLineString, Nullable(QBit(Float64, 4))))
) ENGINE = Memory;
INSERT INTO t_qbit_ws_full SELECT NULL;
SELECT * FROM t_qbit_ws_full;
DROP TABLE t_qbit_ws_full;

SET allow_experimental_nullable_tuple_type = 1;
SELECT ifNull(CAST(NULL AS Nullable(Tuple(Map(String, Nullable(QBit(Float32, 8)))))),
              CAST(tuple(map()) AS Tuple(Map(String, Nullable(QBit(Float32, 8))))));

-- Negative cases: QBits with non-matching dimensions are different types and must
-- not be conflated. Dimensions 1 and 8 are particularly interesting because both
-- pad to a single byte in the underlying `FixedString` storage, so the dimension
-- field is what `structureEquals` relies on to tell them apart.
SELECT CAST(CAST([1.0] AS QBit(Float64, 1)) AS QBit(Float64, 8)); -- { serverError TYPE_MISMATCH }
SELECT CAST(CAST([1., 2., 3., 4., 5., 6., 7., 8.] AS QBit(Float64, 8)) AS QBit(Float64, 1)); -- { serverError TYPE_MISMATCH }
SELECT CAST(CAST([1., 2., 3., 4.] AS QBit(Float64, 4)) AS QBit(Float64, 8)); -- { serverError TYPE_MISMATCH }
