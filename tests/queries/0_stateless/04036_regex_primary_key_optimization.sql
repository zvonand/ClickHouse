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

SELECT count() FROM test_regex_pk WHERE match(id, '^vector');
SELECT count() FROM test_regex_pk WHERE match(id, '^(vector-abc-001|vector-abc-002)');
SELECT count() FROM test_regex_pk WHERE match(id, '^vector-abc-001\\|vector');
SELECT count() FROM test_regex_pk WHERE match(id, '^(vector-abc-001|metrics-def-003)'); -- {serverError INDEX_NOT_USED}
SELECT count() FROM test_regex_pk WHERE match(id, '^vector|^metrics'); -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_pk;
