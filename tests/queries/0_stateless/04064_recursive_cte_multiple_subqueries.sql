-- https://github.com/ClickHouse/ClickHouse/issues/84318
-- This query causes a logical error with recursive CTEs referencing other CTEs.
-- When the issue is fixed, remove the serverError annotation and keep the test as a regression test.
WITH RECURSIVE
    subquery1 AS
    (
        SELECT 1 AS x
        UNION ALL
        SELECT x + 1 AS level
        FROM subquery1
        WHERE x < 5
    ),
    subquery2 AS
    (
        SELECT 1 AS id
        FROM subquery1
    ),
    subquery3 AS
    (
        SELECT id
        FROM subquery2
        UNION ALL
        SELECT cc.id
        FROM subquery3 AS cc
        INNER JOIN subquery2 AS oe ON cc.id = oe.id
        UNION ALL
        SELECT cc.id
        FROM subquery3 AS cc
    )
SELECT *
FROM subquery3
FORMAT Null
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'; -- { serverError LOGICAL_ERROR }
