-- Reproducer for misaligned access in AggregateFunctionForEach with nested
-- combinators whose sizeOfData is not a multiple of alignOfData (e.g. Distinct).
-- The ForEach combinator stores an array of nested states; without proper stride
-- padding, the second and subsequent elements can be misaligned, triggering UBSan.

SELECT sumDistinctForEach(x) FROM (SELECT [number, number % 3] AS x FROM numbers(10));
SELECT countDistinctForEach(x) FROM (SELECT [number % 5, number % 3, number % 2] AS x FROM numbers(30));
