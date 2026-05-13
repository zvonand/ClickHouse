-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104840
--
-- `KeyCondition::applyFunctionChainToColumn` used to call `assert_cast<const ColumnNullable &>`
-- on the result of `func->execute` after stripping outer `LowCardinality` but NOT outer `Const`.
-- For partition expressions where the chain collapses to a single constant value (e.g.
-- `floor(NULL, x)` always returns NULL), the function returned `ColumnConst(ColumnNullable(...))`
-- of size 1. `ColumnConst::isNullable` reports the wrapped column's nullability, so the
-- `isNullable()` guard accepted the column while the subsequent `assert_cast` aborted the
-- server with:
--
--     Bad cast from type DB::ColumnConst to DB::ColumnNullable
--
-- (STID: 3520-4237 in the AST fuzzer infra)

DROP TABLE IF EXISTS t_104840;

CREATE TABLE t_104840
(
    myDay Date NULL,
    myOrder DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY floor(NULL, toRelativeYearNum(myDay))
ORDER BY myOrder
SETTINGS allow_nullable_key = 1, allow_suspicious_indices = 1;

INSERT INTO t_104840 VALUES ('2021-01-01', 1);
INSERT INTO t_104840 VALUES ('2021-01-02', 2);
INSERT INTO t_104840 VALUES ('2021-01-03', 3);

-- The WHERE filter triggers partition pruning, which goes through
-- `canConstantBeWrappedByMonotonicFunctions` -> `applyFunctionChainToColumn`.
-- Before the fix this aborted the server; now it returns the matching row.
SELECT myDay, myOrder FROM t_104840 WHERE myDay = '2021-01-02' ORDER BY myOrder;

DROP TABLE t_104840;
