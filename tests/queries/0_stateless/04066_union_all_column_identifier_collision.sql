-- Reproducer for a bug where UNION ALL branches sharing the same GlobalPlannerContext
-- would throw "Column identifier is already registered" when both branches referenced
-- a table expression with no alias and the same column name.

SELECT * FROM
(
    SELECT uniqExact(m) FROM (SELECT number, CAST(number / 1048576, 'UInt64') AS m FROM numbers(10))
    UNION ALL
    SELECT uniqExact(m) FROM (SELECT number, CAST(number / 2, 'UInt64') AS m FROM numbers(10))
)
ORDER BY 1;
