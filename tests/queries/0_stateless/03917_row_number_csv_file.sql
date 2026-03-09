-- Test _row_number virtual column for CSV format with and without parallel parsing

DROP TABLE IF EXISTS test_csv_row_number;

CREATE TABLE test_csv_row_number (a UInt32, b String) ENGINE = File(CSV);
INSERT INTO test_csv_row_number VALUES (10, 'x'), (20, 'y'), (30, 'z'), (40, 'w'), (50, 'v');

SELECT a, _row_number FROM test_csv_row_number ORDER BY a SETTINGS input_format_parallel_parsing = 0;
SELECT a, _row_number FROM test_csv_row_number ORDER BY a SETTINGS input_format_parallel_parsing = 1;

SELECT _row_number FROM test_csv_row_number ORDER BY _row_number SETTINGS input_format_parallel_parsing = 0;
SELECT _row_number FROM test_csv_row_number ORDER BY _row_number SETTINGS input_format_parallel_parsing = 1;

DROP TABLE test_csv_row_number;
