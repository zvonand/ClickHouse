-- Test that recursive CTE column types are widened via iterative getLeastSupertype.
-- Without type widening, `x` would be UInt8 and the query would run infinitely due to overflow.

SET enable_analyzer = 1;

WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t)
SELECT x, toTypeName(x) FROM t WHERE x >= 256 LIMIT 1;

-- Verify that explicit cast still works as before.
WITH RECURSIVE t AS (SELECT 1::UInt64 AS x UNION ALL SELECT x + 1 FROM t)
SELECT x, toTypeName(x) FROM t WHERE x >= 256 LIMIT 1;

-- With type inference disabled (setting = 0), the type should be UInt8 (old behavior).
-- The query should still work but x will overflow and never reach 256, so we test with a small value.
SET recursive_cte_max_steps_in_type_inference = 0;
WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 10)
SELECT max(x), toTypeName(max(x)) FROM t;
