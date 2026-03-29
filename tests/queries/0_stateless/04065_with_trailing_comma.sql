-- Basic: single WITH element with trailing comma
WITH 1 AS a, SELECT a;

-- Multiple WITH elements with trailing comma
WITH 1 AS a, 2 AS b, SELECT a + b;

-- Trailing comma with subquery alias
WITH (SELECT 1) AS a, SELECT a;

-- Without trailing comma still works
WITH 1 AS a SELECT a;
WITH 1 AS a, 2 AS b SELECT a + b;

-- Named CTE with trailing comma
WITH cte AS (SELECT number FROM numbers(3)), SELECT * FROM cte;

-- Mix of named CTE and scalar expression with trailing comma
WITH 1 AS a, cte AS (SELECT number FROM numbers(3)), SELECT a + sum(number) FROM cte;

-- Materialized CTE with trailing comma
SET enable_materialized_cte = 1;
WITH cte AS MATERIALIZED (SELECT number FROM numbers(3)), SELECT * FROM cte;
SET enable_materialized_cte = 0;

-- Trailing comma in RECURSIVE WITH
WITH RECURSIVE recursive_cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM recursive_cte WHERE n < 3), SELECT * FROM recursive_cte;

-- Nested subquery: trailing comma in inner WITH
SELECT * FROM (WITH 1 AS a, SELECT a);

-- Multiple trailing-comma WITH clauses in UNION
SELECT * FROM (WITH 1 AS a, SELECT a UNION ALL WITH 2 AS b, SELECT b) ORDER BY 1;

-- Double trailing comma should fail
WITH 1 AS a,, SELECT a; -- { serverError SYNTAX_ERROR }

-- Leading comma, no elements should fail
WITH , SELECT 1; -- { serverError SYNTAX_ERROR }
