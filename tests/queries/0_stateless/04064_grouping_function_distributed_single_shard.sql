-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/71270
-- The `grouping` function on a Distributed table with a single shard used to
-- fail with "Method executeImpl is not supported for 'grouping' function".

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.1', numbers(10))
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;

-- Same query with prefer_localhost_replica=0 (forces remote execution path).
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.1', numbers(10))
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr
SETTINGS prefer_localhost_replica = 0;

-- ROLLUP with grouping function on a single-shard distributed table.
SELECT
    number,
    grouping(number) AS gr
FROM remote('127.0.0.1', numbers(5))
GROUP BY ROLLUP(number)
ORDER BY number, gr;

-- CUBE with grouping function on a single-shard distributed table.
SELECT
    number % 2 AS k1,
    number % 3 AS k2,
    grouping(k1, k2) AS gr
FROM remote('127.0.0.1', numbers(6))
GROUP BY CUBE(k1, k2)
ORDER BY k1, k2, gr;
