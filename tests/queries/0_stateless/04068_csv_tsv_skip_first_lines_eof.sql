-- Verify that skip_first_lines doesn't cause an infinite loop when the file has fewer lines than requested.

SELECT * FROM format(CSV, 'x UInt32', '1\n2\n') SETTINGS input_format_csv_skip_first_lines = 100;
SELECT * FROM format(TSV, 'x UInt32', '1\n2\n') SETTINGS input_format_tsv_skip_first_lines = 100;

SELECT * FROM format(CSV, 'x UInt32', '') SETTINGS input_format_csv_skip_first_lines = 1;
SELECT * FROM format(TSV, 'x UInt32', '') SETTINGS input_format_tsv_skip_first_lines = 1;
