-- Reproducer for a bug where MaterializingCTETransform didn't drop totals/extremes,
-- causing Block structure mismatch when uniting CTE pipelines.
WITH cte AS MATERIALIZED (
    SELECT sum(number) FROM numbers(3) GROUP BY number % 2 WITH TOTALS
    UNION ALL
    SELECT sum(number) FROM numbers(5) GROUP BY number % 2 WITH TOTALS
)
SELECT * FROM cte, cte ORDER BY ALL;
