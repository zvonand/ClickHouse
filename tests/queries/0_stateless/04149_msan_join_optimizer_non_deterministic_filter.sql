-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100469 (STID: 1499-4a82).
--
-- The JOIN-conversion optimizers `tryConvertAnyOuterJoinToInnerJoin` (`convertOuterJoinToInnerJoin.cpp`)
-- and `tryConvertAnyJoinToSemiOrAntiJoin` (`convertAnyJoinToSemiOrAntiJoin.cpp`) call
-- `filterResultForNotMatchedRows` / `filterResultForMatchedRows` (in
-- `Processors/QueryPlan/Optimizations/Utils.cpp` and `convertAnyJoinToSemiOrAntiJoin.cpp`)
-- to evaluate the filter against synthetic default-input rows via `ActionsDAG::evaluatePartialResult`
-- with `skip_materialize=true, allow_unknown_function_arguments=true`.
--
-- The dry-run path (`IFunction::executeImplDryRun`) of stateful / non-deterministic functions like
-- `rowNumberInAllBlocks` returns a column allocated but never filled (`ColumnUInt64::create(input_rows_count)`
-- with no `data[i] = ...`). When the result is then read via `getFilterResult` -> `getBool(0)`,
-- MemorySanitizer reports a use-of-uninitialized-value. Independently, the optimizer makes a
-- JOIN-conversion decision based on the garbage byte, which can silently convert ANY OUTER JOIN to
-- INNER JOIN (or ANY JOIN to SEMI/ANTI JOIN) and drop rows that should have been kept.
--
-- The fix adds `dagContainsNonDeterministicFunction` to both partial-evaluation entry points and
-- bails out with `FilterResult::UNKNOWN`, leaving the JOIN unchanged.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1), (2);
INSERT INTO t_r VALUES (1);

-- ANY LEFT JOIN with a filter that contains `rowNumberInAllBlocks` (non-deterministic).
-- Both rows from t_l must be returned; the unmatched row gets default 0 from t_r.
-- Without the fix, the optimizer reads uninitialized memory while deciding whether to convert
-- ANY OUTER JOIN to INNER JOIN -- depending on garbage, it may drop the (2, 0) row.
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

-- ANY LEFT JOIN with `rand` in the filter -- another non-deterministic function. The filter is
-- always true (the modulus is non-negative), so both rows from t_l must be kept; without the fix,
-- the optimizer may incorrectly convert to SEMI JOIN and drop the unmatched (2, 0) row.
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
