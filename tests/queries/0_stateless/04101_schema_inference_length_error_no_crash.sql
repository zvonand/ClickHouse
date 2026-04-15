-- Tags: no-fasttest, no-random-settings
-- Regression test: std::length_error during schema inference should not crash
-- the server. Previously, vector::reserve() throwing std::length_error (a
-- std::logic_error subtype) would cause abort in debug/sanitizer builds
-- because getCurrentExceptionMessage() treats all std::logic_error as
-- assertion failures.

-- Create a tiny file with arbitrary data (not valid MsgPack).
INSERT INTO FUNCTION file('04101_test.bin', 'RawBLOB') VALUES ('hello');

-- Setting input_format_msgpack_number_of_columns to a value exceeding
-- vector::max_size() triggers std::length_error in MsgPackSchemaReader::
-- readRowAndGetDataTypes() via data_types.reserve(number_of_columns).
-- This must return a proper CANNOT_EXTRACT_TABLE_STRUCTURE error, not crash.
SELECT * FROM file('04101_test.bin', 'MsgPack') SETTINGS input_format_msgpack_number_of_columns = 1152921504606846976; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Verify less extreme but still huge values also don't crash (bad_alloc path).
SELECT * FROM file('04101_test.bin', 'MsgPack') SETTINGS input_format_msgpack_number_of_columns = 999999999999999; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
