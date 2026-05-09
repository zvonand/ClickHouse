INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_1.csv', 'CSV', 'x UInt64')
SELECT 1
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_2.csv', 'CSV', 'x UInt64')
SELECT 2
SETTINGS engine_file_truncate_on_insert = 1;

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file([currentDatabase() || '_file_array_arg_1.csv', currentDatabase() || '_file_array_arg_2.csv'], 'CSV', 'x UInt64')
    ORDER BY x
);

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file(arrayConcat([currentDatabase() || '_file_array_arg_1.csv'], [currentDatabase() || '_file_array_arg_2.csv']), auto, 'x UInt64')
    ORDER BY x
);

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file([currentDatabase() || '_file_array_arg_*.csv'], 'CSV', 'x UInt64')
    ORDER BY x
);

SELECT * FROM file([], 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM file([1, 2], 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

INSERT INTO FUNCTION file([currentDatabase() || '_file_array_arg_insert_1.csv', currentDatabase() || '_file_array_arg_insert_2.csv'], 'CSV', 'x UInt64')
SELECT 1; -- { serverError DATABASE_ACCESS_DENIED }
