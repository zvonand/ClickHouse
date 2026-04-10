-- https://github.com/ClickHouse/ClickHouse/issues/102051
-- sz_find_skylake OOB read when needle is all null bytes
SELECT countSubstrings(toString(number), '\0\0\0\0') FROM numbers(100) FORMAT Null;
