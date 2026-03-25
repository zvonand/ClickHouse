-- Regression test for assertion failure when aggregate function combinators
-- wrap AggregateFunctionNothing that carries parameters.
-- When all arguments are NULL, the Null combinator creates AggregateFunctionNothingNull
-- with the original parameters. AggregateFunctionNothingImpl must preserve these parameters
-- so outer combinator wrappers (like Array) stay consistent with nested_func->getParameters().
-- https://github.com/ClickHouse/ClickHouse/issues/100584

-- These queries used to crash with: Assertion `parameters == nested_func->getParameters()' failed
SELECT quantileIfArrayArray(0.5)([[NULL]], [[1]]);
SELECT quantileIfArray(0.5)([NULL], [1]);
SELECT medianIfArrayArray(0.5)([[NULL]], [[1]]);

-- Non-null case must still return correct results
SELECT quantileIfArrayArray(0.5)([[1,2,3]], [[1,1,1]]);
