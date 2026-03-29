SELECT arrayTranspose([[1, 2, 3], [4, 5, 6]]);
SELECT arrayTranspose([[1, 2], [3, 4], [5, 6]]);
SELECT arrayTranspose([[1, 2], [3, 4]]);

-- Single row or column
SELECT arrayTranspose([[1, 2, 3]]);
SELECT arrayTranspose([[1], [2], [3]]);
SELECT arrayTranspose([[42]]);

-- Empty arrays
SELECT arrayTranspose([]::Array(Array(Int64)));
SELECT arrayTranspose([[]]);
SELECT arrayTranspose([[], []]);

-- Floating point
SELECT arrayTranspose([[1.5, 2.5], [3.5, 4.5]]);

-- Strings
SELECT arrayTranspose([['a', 'b', 'c'], ['d', 'e', 'f']]);

-- Nullable elements
SELECT arrayTranspose([[1, NULL, 3], [4, 5, NULL]]);

-- Multiple rows
SELECT arrayTranspose(a) FROM (
    SELECT [[1, 2], [3, 4]] AS a
    UNION ALL
    SELECT [[5, 6, 7], [8, 9, 10], [11, 12, 13]] AS a
) ORDER BY a;

-- Double transpose returns the initial input
SELECT arrayTranspose(arrayTranspose([[1, 2, 3], [4, 5, 6]]));

-- Error: inner arrays of different sizes
SELECT arrayTranspose([[1, 2], [3]]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Error: not a 2D array
SELECT arrayTranspose([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
