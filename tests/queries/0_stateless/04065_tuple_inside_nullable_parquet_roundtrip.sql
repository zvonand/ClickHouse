-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;

-- Nullable struct with non-nullable elements
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), (NULL), ((3, 'c'));

-- Parquet Arrow reader
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Both struct and element nullable: Nullable(Tuple(Nullable(UInt32), String))
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(Nullable(UInt32), String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), (NULL), ((NULL, 'c')), ((4, 'd'));

-- Parquet Arrow reader both nullable
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_both.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_both.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_both.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Non-nullable struct with nullable elements
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Tuple(Nullable(UInt32), String)) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), ((NULL, 'b'));

-- Parquet Arrow reader nullable elements
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_elem.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_elem.parquet', 'Parquet', 'c0 Tuple(Nullable(UInt32), String)') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader nullable elements
SELECT c0 FROM file(currentDatabase() || '_04065_elem.parquet', 'Parquet', 'c0 Tuple(Nullable(UInt32), String)') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_tuple_inside_nullable;

-- Plain non-nullable tuple
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), ((2, 'b'));

-- Parquet Arrow reader plain
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_plain.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_plain.parquet', 'Parquet', 'c0 Tuple(UInt32, String)') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader plain
SELECT c0 FROM file(currentDatabase() || '_04065_plain.parquet', 'Parquet', 'c0 Tuple(UInt32, String)') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_tuple_inside_nullable;

-- Named tuple
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(a UInt32, b String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'x')), (NULL), ((3, 'z'));

-- Parquet Arrow reader named
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_named.parquet', 'Parquet', 'c0 Nullable(Tuple(a UInt32, b String))') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_named.parquet', 'Parquet', 'c0 Nullable(Tuple(a UInt32, b String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader named (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_named.parquet', 'Parquet', 'c0 Nullable(Tuple(a UInt32, b String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- All-NULL column
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES (NULL), (NULL), (NULL);

-- Parquet Arrow reader all null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_allnull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_allnull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader all null (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_allnull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- No-NULL column (nullable type, zero actual NULLs)
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), ((2, 'b')), ((3, 'c'));

-- Parquet Arrow reader no null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_nonull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_nonull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader no null (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_nonull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Single-element tuple
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1,)), (NULL), ((3,));

-- Parquet Arrow reader single
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_single.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32))') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_single.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader single (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_single.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Deeply nested: nullable tuple inside nullable tuple
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES (((1, 'a'), 10)), (NULL), ((NULL, 20)), (((4, 'd'), 40));

-- Parquet Arrow reader deep nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_deep.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_deep.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader deep nested (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_deep.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Nullable tuple with Array element
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(Array(UInt32), String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES (([1, 2], 'a')), (NULL), (([3], 'c'));

-- Parquet Arrow reader array elem
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_arr.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_arr.parquet', 'Parquet', 'c0 Nullable(Tuple(Array(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader array elem (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_arr.parquet', 'Parquet', 'c0 Nullable(Tuple(Array(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Multiple nullable tuple columns
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a'), (1.5)), (NULL, (2.5)), ((3, 'c'), NULL);

-- Parquet Arrow reader multi col
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_multi.parquet', 'Parquet') SELECT c0, c1 FROM test_tuple_inside_nullable;
SELECT c0, c1 FROM file(currentDatabase() || '_04065_multi.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader multi col (not yet supported)
SELECT c0, c1 FROM file(currentDatabase() || '_04065_multi.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Schema inference without type hint (works for both readers, but V3 loses struct-level NULL)
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), (NULL), ((3, 'c'));

-- Parquet Arrow reader infer
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_infer.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;
SELECT c0 FROM file(currentDatabase() || '_04065_infer.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

DROP TABLE test_tuple_inside_nullable;

-- Type hint mismatch: file has Nullable(Tuple(...)), read as Tuple(...) (strip nullable, NULLs become defaults)
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), (NULL), ((3, 'c'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_mismatch1.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;

-- Parquet Arrow reader: read nullable file as non-nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_mismatch1.parquet', 'Parquet', 'c0 Tuple(UInt32, String)') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

DROP TABLE test_tuple_inside_nullable;

-- Type hint mismatch: file has Tuple(...), read as Nullable(Tuple(...)) (add nullable wrapper)
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), ((2, 'b'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_mismatch2.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;

-- Parquet Arrow reader: read non-nullable file as nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_mismatch2.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader: read non-nullable file as nullable (not yet supported)
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_mismatch2.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_tuple_inside_nullable;

-- Schema inference: inferred type with toTypeName
DROP TABLE IF EXISTS test_tuple_inside_nullable;
CREATE TABLE test_tuple_inside_nullable (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_tuple_inside_nullable VALUES ((1, 'a')), (NULL), ((3, 'c'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_describe.parquet', 'Parquet') SELECT c0 FROM test_tuple_inside_nullable;

-- Parquet Arrow reader: inferred type
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_describe.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader: inferred type (struct-level NULL not supported, becomes (NULL,NULL))
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_describe.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_tuple_inside_nullable;
