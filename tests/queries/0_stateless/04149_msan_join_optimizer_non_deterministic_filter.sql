-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100469 (STID: 1499-4a82).
--
-- The JOIN-conversion optimizers `tryConvertAnyOuterJoinToInnerJoin` (`convertOuterJoinToInnerJoin.cpp`)
-- and `tryConvertAnyJoinToSemiOrAntiJoin` (`convertAnyJoinToSemiOrAntiJoin.cpp`) call
-- `filterResultForNotMatchedRows` / `filterResultForMatchedRows` (in
-- `Processors/QueryPlan/Optimizations/Utils.cpp` and `convertAnyJoinToSemiOrAntiJoin.cpp`)
-- to evaluate the filter against synthetic default-input rows via `ActionsDAG::evaluatePartialResult`
-- with `skip_materialize=true, allow_unknown_function_arguments=true`. Internally this calls
-- `IFunction::executeImplDryRun` for every function in the filter DAG.
--
-- The dry-run override of `rowNumberInAllBlocks` previously returned `ColumnUInt64::create(input_rows_count)`
-- without filling the buffer, so when the optimizer read the result via `getFilterResult` -> `getBool(0)`
-- MemorySanitizer reported a use-of-uninitialized-value. The fix initializes the dry-run output column
-- with zeros at the source (`Functions/rowNumberInAllBlocks.cpp`) so the column produced by the function
-- is fully initialized, which is the contract callers of `executeImplDryRun` rely on.
--
-- The queries below exercise the partial-evaluation chain with `rowNumberInAllBlocks` and `rand` in the
-- filter and must run cleanly under MemorySanitizer.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1), (2);
INSERT INTO t_r VALUES (1);

-- ANY LEFT JOIN with a filter that contains `rowNumberInAllBlocks` (stateful, non-deterministic).
-- Both rows from t_l must be returned; the unmatched row gets default 0 from t_r.
-- Without the fix, the optimizer reads uninitialized memory from the dry-run output of
-- `rowNumberInAllBlocks` while deciding whether to convert ANY OUTER JOIN to INNER JOIN.
SELECT t_l.id, t_r.id
FROM t_l ANY LEFT JOIN t_r ON t_l.id = t_r.id
WHERE rowNumberInAllBlocks() < 100
ORDER BY t_l.id;

-- Symmetric ANY RIGHT JOIN -- exercises the right-stream branch of `tryConvertAnyOuterJoinToInnerJoin`.
DROP TABLE t_l;
DROP TABLE t_r;
CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1);
INSERT INTO t_r VALUES (1), (2);

SELECT t_l.id, t_r.id
FROM t_l ANY RIGHT JOIN t_r ON t_l.id = t_r.id
WHERE rowNumberInAllBlocks() < 100
ORDER BY t_r.id;

-- ANY LEFT JOIN with `rand` in the filter -- another non-deterministic function whose dry-run
-- output flows into the same partial-evaluation chain. The filter is always true (the modulus is
-- non-negative), so both rows from t_l must be kept.
DROP TABLE t_l;
DROP TABLE t_r;
CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1), (2);
INSERT INTO t_r VALUES (1);

SELECT t_l.id, t_r.id
FROM t_l ANY LEFT JOIN t_r ON t_l.id = t_r.id
WHERE (rand() % 1000) >= 0
ORDER BY t_l.id;

-- PedroTadim's original simplified reproducer (issue #100469): no explicit JOIN, but
-- exercises a similar partial-evaluation chain. Must run cleanly without MSan reports.
DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (p Int) ENGINE = Memory;
INSERT INTO t0 VALUES (0);
SELECT 1 FROM t0 WHERE (SELECT p = 1) AND rowNumberInAllBlocks() = 1;

DROP TABLE t0;
