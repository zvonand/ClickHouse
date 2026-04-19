-- Tags: no-fasttest
-- no-fasttest: requires dictionaries

SET enable_analyzer = 1; -- DictGetTupleElementPass is an Analyzer pass; EXPLAIN QUERY TREE also requires the analyzer

DROP TABLE IF EXISTS test_keys;
DROP DICTIONARY IF EXISTS test_dict;
DROP TABLE IF EXISTS dict_source;

CREATE TABLE dict_source
(
    id UInt64,
    country String,
    city String,
    population UInt64
) ENGINE = Memory;

INSERT INTO dict_source VALUES (1, 'US', 'New York', 8336000), (2, 'FR', 'Paris', 2161000), (3, 'JP', 'Tokyo', 13960000);

CREATE DICTIONARY test_dict
(
    id UInt64,
    country String,
    city String,
    population UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase()))
LAYOUT(FLAT())
LIFETIME(0);

CREATE TABLE test_keys (id UInt64) ENGINE = Memory;
INSERT INTO test_keys VALUES (1), (2), (3);

-- With optimization: the query tree should contain dictGet but not tupleElement
SELECT 'optimization enabled';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 1) FROM test_keys
    SETTINGS optimize_dictget_tuple_element = 1
) WHERE explain LIKE '%function_name: dictGet,%';
SELECT count() FROM (
    EXPLAIN QUERY TREE
    SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 1) FROM test_keys
    SETTINGS optimize_dictget_tuple_element = 1
) WHERE explain LIKE '%function_name: tupleElement%';

-- Without optimization: tupleElement should be present wrapping dictGet
SELECT 'optimization disabled';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 1) FROM test_keys
    SETTINGS optimize_dictget_tuple_element = 0
) WHERE explain LIKE '%function_name: tupleElement%';

-- Functional tests: verify correctness
SELECT 'dictGet index access';
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city'), id).1 FROM test_keys ORDER BY id;
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city'), id).2 FROM test_keys ORDER BY id;

SELECT 'all three attributes by index';
SELECT
    dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).1,
    dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).2,
    dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).3
FROM test_keys ORDER BY id;

-- Test named access (e.g. .country instead of .1)
SELECT 'named access';
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).country FROM test_keys ORDER BY id;
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).city FROM test_keys ORDER BY id;

-- Test with dictGetOrDefault (existing keys)
SELECT 'dictGetOrDefault';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), id, ('Unknown', 'Unknown')).1 FROM test_keys ORDER BY id;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), id, ('Unknown', 'Unknown')).2 FROM test_keys ORDER BY id;

-- Test with dictGetOrDefault (missing keys — exercises the default value rewrite path)
SELECT 'dictGetOrDefault with missing keys';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).2;

-- Test dictGetOrDefault with named access on missing keys (exercises the default-value rewrite path with string index)
SELECT 'dictGetOrDefault with missing keys named access';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).country;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).city;

-- Test dictGetOrDefault with tuple() function as default
SELECT 'dictGetOrDefault with tuple function default';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('FuncCountry', 'FuncCity')).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('FuncCountry', 'FuncCity')).2;

-- Test dictGetOrDefault with non-rewritable default (non-constant expression) — the pass must bail out gracefully
SELECT 'dictGetOrDefault with non-rewritable default';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), materialize(('MCountry', 'MCity'))).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), materialize(('MCountry', 'MCity'))).2;

-- Test shared-parent scenario: ORDER BY ALL references the SELECT expression, so the tupleElement
-- (and its inner dictGet) node is shared between SELECT and ORDER BY. The pass must not mutate the
-- shared dictGet in place — that would leave the other parent's tupleElement wrapping a scalar.
SELECT 'shared tupleElement across SELECT and ORDER BY ALL';
SELECT DISTINCT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 'city') FROM test_keys ORDER BY ALL;

DROP TABLE test_keys;
DROP DICTIONARY test_dict;
DROP TABLE dict_source;
