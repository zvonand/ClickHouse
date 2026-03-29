-- Basic: single WITH element with trailing comma
WITH 1 AS a, SELECT a;

-- Multiple WITH elements with trailing comma
WITH 1 AS a, 2 AS b, SELECT a + b;

-- Trailing comma with subquery alias
WITH (SELECT 1) AS a, SELECT a;

-- Without trailing comma still works
WITH 1 AS a SELECT a;
WITH 1 AS a, 2 AS b SELECT a + b;

-- Multiple WITH elements, mixed expressions, trailing comma
WITH 1 AS a, (SELECT 2) AS b, 3 AS c, SELECT a + b + c;

-- Trailing comma in RECURSIVE WITH
WITH RECURSIVE recursive_cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM recursive_cte WHERE n < 3), SELECT * FROM recursive_cte;

-- Nested subquery: trailing comma in inner WITH
SELECT * FROM (WITH 1 AS a, SELECT a);

-- Multiple trailing-comma WITH clauses in UNION
WITH 1 AS a, SELECT a UNION ALL WITH 2 AS b, SELECT b;

-- Double trailing comma should fail
WITH 1 AS a,, SELECT a; -- { serverError SYNTAX_ERROR }

-- Leading comma, no elements should fail
WITH , SELECT 1; -- { serverError SYNTAX_ERROR }
