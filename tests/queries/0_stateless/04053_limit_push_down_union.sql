SET enable_analyzer = 1;

-- Test that LIMIT is pushed down into UNION ALL branches.
-- https://github.com/ClickHouse/ClickHouse/issues/23239

-- Simple case: LIMIT pushed into each branch of UNION ALL.
EXPLAIN PLAN header=0
SELECT * FROM
(
    SELECT number FROM numbers(100)
    UNION ALL
    SELECT number FROM numbers(200)
)
LIMIT 5;

SELECT '---';

-- LIMIT with OFFSET: each branch gets LIMIT (limit + offset).
EXPLAIN PLAN header=0
SELECT * FROM
(
    SELECT number FROM numbers(100)
    UNION ALL
    SELECT number FROM numbers(200)
)
LIMIT 3 OFFSET 2;

SELECT '---';

-- Three branches.
EXPLAIN PLAN header=0
SELECT * FROM
(
    SELECT number FROM numbers(100)
    UNION ALL
    SELECT number FROM numbers(200)
    UNION ALL
    SELECT number FROM numbers(300)
)
LIMIT 10;

SELECT '---';

-- Verify correctness: the pushed-down limits should not change the result.
SELECT count() FROM
(
    SELECT * FROM
    (
        SELECT number FROM numbers(100)
        UNION ALL
        SELECT number FROM numbers(200)
    )
    LIMIT 5
);

SELECT '---';

-- Verify LIMIT with OFFSET correctness.
SELECT count() FROM
(
    SELECT * FROM
    (
        SELECT number FROM numbers(100)
        UNION ALL
        SELECT number FROM numbers(200)
    )
    LIMIT 3 OFFSET 2
);

SELECT '---';

-- WITH TIES must not be pushed down (requires ORDER BY, so Sorting sits between Limit and Union).
EXPLAIN PLAN header=0
SELECT * FROM
(
    SELECT number FROM numbers(100)
    UNION ALL
    SELECT number FROM numbers(200)
)
ORDER BY number
LIMIT 5 WITH TIES;

SELECT '---';

-- WITH FILL must not be pushed down (FillingStep sits between Limit and Union).
EXPLAIN PLAN header=0
SELECT * FROM
(
    SELECT number FROM numbers(100)
    UNION ALL
    SELECT number FROM numbers(200)
)
ORDER BY number WITH FILL
LIMIT 5;
