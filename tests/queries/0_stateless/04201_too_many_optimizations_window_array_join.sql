-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101693.
-- The combination of `ARRAY JOIN`, window functions and a filter that references
-- window-output columns used to cause `removeUnusedColumns` and `mergeExpressions`
-- to oscillate forever, eventually exceeding the optimization limit and throwing
-- TOO_MANY_QUERY_PLAN_OPTIMIZATIONS.

DROP TABLE IF EXISTS test_repro_101693;

CREATE TABLE test_repro_101693
(
    sym  String,
    px   Float64,
    qty2 Float64,
    ts   Int64
) ENGINE = MergeTree() ORDER BY (sym, ts);

INSERT INTO test_repro_101693 SELECT 'B', 1.0, 1.0, number FROM numbers(5);

WITH
    expanded AS
    (
        SELECT
            sym, ts, qty2, px, i,
            groupArray(px)   OVER w[i]     AS prev_px1,
            groupArray(px)   OVER w[i + 1] AS prev_px2,
            groupArray(qty2) OVER w[i + 1] AS prev_qty2_1
        FROM test_repro_101693
        ARRAY JOIN range(1, 100) AS i
        WINDOW w AS (PARTITION BY (sym, i) ORDER BY ts)
    )
SELECT count()
FROM
(
    SELECT
        sym, i, px,
        if(any(px) = any(prev_px2), max(qty2) - any(prev_qty2_1), max(qty2)) AS final_qty
    FROM expanded
    WHERE px >= least(prev_px1, prev_px2)
    GROUP BY sym, i, px
)
SETTINGS allow_experimental_analyzer = 1;

DROP TABLE test_repro_101693;
