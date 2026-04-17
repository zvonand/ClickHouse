-- tryConvertAnyJoinToSemiOrAntiJoin used to call ActionsDAG::evaluatePartialResult on a WHERE DAG
-- containing IN (subquery); on debug/sanitizer builds FunctionIn on a not-ready Set aborted via
-- LOGICAL_ERROR before the surrounding try/catch could intercept

DROP TABLE IF EXISTS t_left_04094;
DROP TABLE IF EXISTS t_right_04094;
DROP TABLE IF EXISTS t_subq_04094;

CREATE TABLE t_left_04094  (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_right_04094 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_subq_04094  (x UInt64)           ENGINE = MergeTree ORDER BY x;

INSERT INTO t_left_04094  VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_right_04094 VALUES (1, 100), (2, 200);
INSERT INTO t_subq_04094  VALUES (1);

SELECT a, b, c
FROM t_left_04094
ANY LEFT JOIN t_right_04094 USING (a)
WHERE toUInt64(1 IN (SELECT x FROM t_subq_04094)) > 0
ORDER BY a
SETTINGS
    query_plan_filter_push_down = 0,
    query_plan_split_filter     = 0,
    query_plan_merge_filters    = 0;

SELECT a, b, c
FROM t_right_04094
ANY RIGHT JOIN t_left_04094 USING (a)
WHERE toUInt64(1 IN (SELECT x FROM t_subq_04094)) > 0
ORDER BY a
SETTINGS
    query_plan_filter_push_down = 0,
    query_plan_split_filter     = 0,
    query_plan_merge_filters    = 0;

DROP TABLE t_left_04094;
DROP TABLE t_right_04094;
DROP TABLE t_subq_04094;
