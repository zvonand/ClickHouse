-- Tags: no-fasttest
-- Verify that regex patterns with groups and alternation
-- can use the primary key index for range filtering.
-- See https://github.com/ClickHouse/ClickHouse/issues/82159

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

SET force_primary_key = 1;

-- Test 1: Simple anchored regex — should use primary key (already worked before)
SELECT count() FROM test_regex_pk WHERE match(id, '^vector');

-- Test 2: Anchored regex with group and common prefix — should now use primary key
SELECT count() FROM test_regex_pk WHERE match(id, '^(vector-abc-001|vector-abc-002)');

-- Test 3: Pattern with escaped pipe (literal |) — should use primary key
SELECT count() FROM test_regex_pk WHERE match(id, '^vector-abc-001\\|vector');

-- Test 4: Alternation with NO common prefix — cannot use primary key
SELECT count() FROM test_regex_pk WHERE match(id, '^(vector-abc-001|metrics-def-003)'); -- {serverError INDEX_NOT_USED}

-- Test 5: Bare alternation without group — cannot use primary key
SELECT count() FROM test_regex_pk WHERE match(id, '^vector|^metrics'); -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_pk;
