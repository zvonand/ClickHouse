-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_read_in_order_limit_filter;

CREATE TABLE t_read_in_order_limit_filter (
    Id String,
    Document JSON(TypedField String),
    Payload String DEFAULT repeat('x', 200)
) ENGINE = MergeTree
ORDER BY Id
SETTINGS index_granularity = 8192,
    min_bytes_for_wide_part = 9223372036854775807,
    write_marks_for_substreams_in_compact_parts = 1;

SYSTEM STOP MERGES t_read_in_order_limit_filter;

INSERT INTO t_read_in_order_limit_filter (Id, Document) SELECT
    leftPad(toString(number * 2), 10, '0'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(100000);

INSERT INTO t_read_in_order_limit_filter (Id, Document) SELECT
    leftPad(toString(number * 2 + 1), 10, '0'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(100000);

INSERT INTO t_read_in_order_limit_filter (Id, Document) SELECT
    leftPad(toString(number * 3), 10, '0'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(100000);

SELECT * FROM t_read_in_order_limit_filter ORDER BY Id ASC LIMIT 101
FORMAT Null SETTINGS log_comment = '04201_no_filter';

SELECT * FROM t_read_in_order_limit_filter
WHERE hasAny(Document.ScopeIds, ['aaa', 'all'])
ORDER BY Id ASC LIMIT 101
FORMAT Null SETTINGS log_comment = '04201_with_filter';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, read_rows < 100_000
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('04201_no_filter', '04201_with_filter')
  AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_read_in_order_limit_filter;
