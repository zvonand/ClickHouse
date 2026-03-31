-- Tags: no-parallel

DROP TABLE IF EXISTS t_compact_writer_cancel;

CREATE TABLE t_compact_writer_cancel (key UInt64, value String)
ENGINE = MergeTree()
ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 10000000;

SYSTEM ENABLE FAILPOINT compact_writer_add_streams_throw;

INSERT INTO t_compact_writer_cancel VALUES (1, 'hello'); -- { serverError FAULT_INJECTED }

-- The failpoint is ONCE, so subsequent inserts must succeed.
INSERT INTO t_compact_writer_cancel VALUES (2, 'world');

SELECT * FROM t_compact_writer_cancel ORDER BY key;

DROP TABLE t_compact_writer_cancel;
