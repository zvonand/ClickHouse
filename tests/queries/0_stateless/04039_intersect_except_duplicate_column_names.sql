-- Regression test: EXCEPT/INTERSECT with duplicate column names caused a segfault
-- because `IntersectOrExceptTransform` used `getPositionByName` which returns the
-- first position for a given name, making some actual columns unreferenced.

-- Duplicate Array columns that caused a segfault before the fix.
(SELECT materialize([])::Array(Int64), materialize(42) AS b, materialize([])::Array(Int64) FROM numbers(100))
EXCEPT DISTINCT
(SELECT materialize([])::Array(Int64), materialize(42) AS b, materialize([])::Array(Int64) FROM numbers(100))
FORMAT Null;

(SELECT materialize([])::Array(Int64), materialize(42) AS b, materialize([])::Array(Int64) FROM numbers(100))
INTERSECT DISTINCT
(SELECT materialize([])::Array(Int64), materialize(42) AS b, materialize([])::Array(Int64) FROM numbers(100))
FORMAT Null;

-- EXCEPT ALL / INTERSECT ALL with duplicate column names.
(SELECT number % 2 AS a, number % 3 AS b, number % 2 AS a FROM numbers(3))
EXCEPT ALL
(SELECT number % 2 AS a, number % 3 AS b, number % 2 AS a FROM numbers(1));

(SELECT number % 2 AS a, number % 3 AS b, number % 2 AS a FROM numbers(3))
INTERSECT ALL
(SELECT number % 2 AS a, number % 3 AS b, number % 2 AS a FROM numbers(1));

-- Three duplicate columns.
(SELECT 1 AS x, 1 AS x, 1 AS x) EXCEPT DISTINCT (SELECT 1 AS x, 1 AS x, 1 AS x);
(SELECT 1 AS x, 1 AS x, 1 AS x) INTERSECT DISTINCT (SELECT 1 AS x, 1 AS x, 1 AS x);
