-- Verify that skip_first_lines doesn't cause an infinite loop when the file has fewer lines than requested.
-- The bug only reproduces with file-based reading (pread), not with inline format() which uses ReadBufferFromMemory.
-- Use a very large skip value to trigger the infinite loop on unfixed builds; the test runner timeout catches the hang.

INSERT INTO FUNCTION file(currentDatabase() || '_04068.csv', 'CSV') SELECT number FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_04068.csv', 'CSV', 'x UInt64') SETTINGS input_format_csv_skip_first_lines = 1000000000;

INSERT INTO FUNCTION file(currentDatabase() || '_04068.tsv', 'TSV') SELECT number FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_04068.tsv', 'TSV', 'x UInt64') SETTINGS input_format_tsv_skip_first_lines = 1000000000;
