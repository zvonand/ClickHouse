DROP TABLE IF EXISTS test_regex_pk;

CREATE TABLE test_regex_pk
(
    id String
)
ENGINE = MergeTree()
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_regex_pk VALUES
    ('vector-abc-001'), ('vector-abc-002'),
    ('metrics-def-003'), ('metrics-def-004'),
    ('logs-ghi-005'), ('traces-jkl-007');

-- Simple prefix: reads only vector-abc-* granules (MergeTree may read one extra mark at boundary)
SELECT count() FROM test_regex_pk WHERE match(id, '^vector') SETTINGS force_primary_key = 1, max_rows_to_read = 3;

-- Alternation with common prefix "vector-abc-00": reads only vector-abc-* granules
SELECT count() FROM test_regex_pk WHERE match(id, '^(vector-abc-001|vector-abc-002)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;

-- Escaped pipe (literal |): prefix "vector-abc-001|vector" matches no rows
SELECT count() FROM test_regex_pk WHERE match(id, '^vector-abc-001\\|vector') SETTINGS force_primary_key = 1;

-- Alternation without common prefix: no optimization possible
SELECT count() FROM test_regex_pk WHERE match(id, '^(vector-abc-001|metrics-def-003)'); -- {serverError INDEX_NOT_USED}

-- Multiple anchors without grouping: no optimization possible
SELECT count() FROM test_regex_pk WHERE match(id, '^vector|^metrics'); -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_pk;
