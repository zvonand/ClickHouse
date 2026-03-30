-- https://github.com/ClickHouse/ClickHouse/issues/85465
SELECT 1 x, lag(1) OVER () GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls = 1;
SELECT toInt32OrDefault('a') x, min(x) OVER () GROUP BY CUBE(x) WITH TOTALS SETTINGS group_by_use_nulls = 1;
