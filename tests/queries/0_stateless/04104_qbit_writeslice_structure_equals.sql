-- Tags: no-fasttest
-- Tag `no-fasttest`: `QBit` depends on non-fasttest libraries.
--
-- Regression test: `ColumnQBit::structureEquals` previously ignored the right-hand side
-- `ColumnQBit` wrapper and delegated the comparison to the inner tuple with a
-- `ColumnQBit` as the argument, which always returned `false`. Any operation using
-- `structureEquals` to validate two equivalent `ColumnQBit` columns then failed; the
-- most visible case was `writeSlice(GenericArraySlice, GenericArraySink)` throwing a
-- `LOGICAL_ERROR` when evaluating `if`/`ifNull` over tuples or maps containing a `QBit`
-- element.
--
-- Reproducer: inserting `NULL` into a column whose target type contains a `QBit` nested
-- inside a `Map` inside a `Tuple` goes through `ConvertingTransform` -> `ifNull` -> `if`
-- -> `executeTuple` -> `executeMap` -> `executeGenericArray` -> `writeSlice`. Before
-- the fix this raised the exception
--   Logical error: Function writeSlice expects same column types for
--   GenericArraySlice and GenericArraySink.
-- After the fix the row is converted to the default tuple value and written normally.

DROP TABLE IF EXISTS t_qbit_ws;
CREATE TABLE t_qbit_ws (c0 Tuple(Map(String, Nullable(QBit(Float64, 4))))) ENGINE = Memory;
INSERT INTO t_qbit_ws SELECT NULL;
SELECT * FROM t_qbit_ws;
DROP TABLE t_qbit_ws;

-- Same pattern with a richer tuple shape as produced by the BuzzHouse fuzzer that
-- originally surfaced the bug (STID 2670-0e56).
DROP TABLE IF EXISTS t_qbit_ws_full;
CREATE TABLE t_qbit_ws_full (
    c0 Tuple(Nullable(Int8), Enum16('a' = 1, 'b' = 2), Map(MultiLineString, Nullable(QBit(Float64, 4))))
) ENGINE = Memory;
INSERT INTO t_qbit_ws_full SELECT NULL;
SELECT * FROM t_qbit_ws_full;
DROP TABLE t_qbit_ws_full;

-- `ifNull` over a tuple containing `QBit` directly should also succeed now that
-- `ColumnQBit::structureEquals` correctly compares two `ColumnQBit` columns.
SET allow_experimental_nullable_tuple_type = 1;
SELECT ifNull(CAST(NULL AS Nullable(Tuple(Map(String, Nullable(QBit(Float32, 8)))))),
              CAST(tuple(map()) AS Tuple(Map(String, Nullable(QBit(Float32, 8))))));
