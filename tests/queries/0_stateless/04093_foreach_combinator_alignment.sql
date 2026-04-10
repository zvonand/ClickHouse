-- Regression test: -ForEach with -OrDefault/-OrNull/-Distinct nested combinators
-- must not trigger UBSan misaligned-address errors.
-- The issue was that AggregateFunctionOrFill::sizeOfData() returned an
-- unpadded value, so the second element in the ForEach array of aggregate
-- states was misaligned.

-- OrDefault + Distinct (original failing combo)
SELECT countOrDefaultDistinctForEach([1, 2, 3]);
SELECT sumOrDefaultDistinctForEach([10, 20]);
-- OrNull
SELECT sumOrNullForEach([100, 200, 300]);
-- OrDefault / OrNull without Distinct
SELECT sumOrDefaultForEach([1, 2, 3]) FROM (SELECT * FROM numbers(3));
SELECT sumOrNullForEach([1, 2]) FROM (SELECT * FROM numbers(3));
-- OrNull + Distinct
SELECT countOrNullDistinctForEach([10, 20, 30]);
