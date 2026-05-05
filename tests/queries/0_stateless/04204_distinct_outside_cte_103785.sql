-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103785
-- DISTINCT inside a CTE branch was being silently weakened when an outer
-- query projected only a subset of the CTE's columns. The new analyzer's
-- `RemoveUnusedProjectionColumnsPass` correctly skipped DISTINCT
-- query-as-FROM-children, but did not skip DISTINCT query-as-children
-- of a `UNION ALL`, so the unused column was pruned out of the inner
-- DISTINCT projection — turning `SELECT DISTINCT ID, Qty` into
-- effectively `SELECT DISTINCT Qty` and dropping rows that share the
-- same `Qty` value but have different `ID`.

DROP TABLE IF EXISTS t_qty_103785;
CREATE TABLE t_qty_103785 (ID UInt32, Qty UInt64) ENGINE = Memory;
INSERT INTO t_qty_103785 VALUES (115, 100000), (116, 130000), (117, 150000), (118, 150000);

SELECT '-- Query A: full projection (ID, Qty) - was already correct';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT ID, Qty FROM a ORDER BY ID;

SELECT '-- Query B: subset projection (Qty only) - used to drop a row';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT Qty FROM a ORDER BY Qty;

SELECT '-- Query C: subset projection through nested SELECT - used to drop a row';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT Qty FROM (SELECT ID, Qty FROM a) ORDER BY Qty;

SELECT '-- Query D: DISTINCT in second branch of UNION ALL';
WITH a AS (
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
    UNION ALL
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
)
SELECT Qty FROM a ORDER BY Qty;

SELECT '-- Query E: DISTINCT inside a nested UNION ALL';
WITH a AS (
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
    UNION ALL
    (
        SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
        UNION ALL
        SELECT ID, Qty FROM t_qty_103785 WHERE ID = 117
    )
)
SELECT Qty FROM a ORDER BY Qty;

SELECT '-- Query F: workaround setting still works as before';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT Qty FROM a ORDER BY Qty SETTINGS enable_analyzer = 0, optimize_duplicate_order_by_and_distinct = 0;

SELECT '-- Query G: existing 03023 case - DISTINCT as direct FROM-child (already worked)';
SELECT product_id
FROM (
    SELECT DISTINCT product_id, section_id
    FROM (
        SELECT
            concat('product_', number % 2) AS product_id,
            concat('section_', number % 3) AS section_id
        FROM numbers(10)
    )
)
ORDER BY product_id;

DROP TABLE t_qty_103785;
