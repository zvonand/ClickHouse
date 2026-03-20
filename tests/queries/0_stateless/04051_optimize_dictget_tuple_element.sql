-- Tags: no-fasttest
-- no-fasttest: requires dictionaries

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
SOURCE(CLICKHOUSE(TABLE 'dict_source'))
LAYOUT(FLAT())
LIFETIME(0);

CREATE TABLE test_keys (id UInt64) ENGINE = Memory;
INSERT INTO test_keys VALUES (1), (2), (3);

-- With optimization: dictGet should have single attribute 'country', no tupleElement wrapper
SELECT 'optimization enabled';
EXPLAIN QUERY TREE
SELECT dictGet('default.test_dict', ('country', 'city', 'population'), id).1 FROM test_keys
SETTINGS optimize_dictget_tuple_element = 1;

-- Without optimization: tupleElement wrapping dictGet with tuple of attributes
SELECT 'optimization disabled';
EXPLAIN QUERY TREE
SELECT dictGet('default.test_dict', ('country', 'city', 'population'), id).1 FROM test_keys
SETTINGS optimize_dictget_tuple_element = 0;

-- Functional tests: verify correctness
SELECT 'dictGet index access';
SELECT dictGet('default.test_dict', ('country', 'city'), id).1 FROM test_keys ORDER BY id;
SELECT dictGet('default.test_dict', ('country', 'city'), id).2 FROM test_keys ORDER BY id;

SELECT 'all three attributes by index';
SELECT
    dictGet('default.test_dict', ('country', 'city', 'population'), id).1,
    dictGet('default.test_dict', ('country', 'city', 'population'), id).2,
    dictGet('default.test_dict', ('country', 'city', 'population'), id).3
FROM test_keys ORDER BY id;

-- Test named access (e.g. .country instead of .1)
SELECT 'named access';
SELECT dictGet('default.test_dict', ('country', 'city', 'population'), id).country FROM test_keys ORDER BY id;
SELECT dictGet('default.test_dict', ('country', 'city', 'population'), id).city FROM test_keys ORDER BY id;

-- Test with dictGetOrDefault
SELECT 'dictGetOrDefault';
SELECT dictGetOrDefault('default.test_dict', ('country', 'city'), id, ('Unknown', 'Unknown')).1 FROM test_keys ORDER BY id;
SELECT dictGetOrDefault('default.test_dict', ('country', 'city'), id, ('Unknown', 'Unknown')).2 FROM test_keys ORDER BY id;

DROP TABLE test_keys;
DROP DICTIONARY test_dict;
DROP TABLE dict_source;
