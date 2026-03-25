-- Tags: no-parallel-replicas

-- Tests that text indexes built on JSONAllValues can be used with JSON subcolumn queries.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    data JSON,
    INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY (id) SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (0, '{"key1": "the quick brown fox", "key2": "hello world"}');
INSERT INTO tab VALUES (1, '{"key1": "lazy dog jumps", "key2": "goodbye world"}');
INSERT INTO tab VALUES (2, '{"key1": "quick silver", "num": 42}');
INSERT INTO tab VALUES (3, '{"key1": "nothing special", "num": 100}');

SELECT '-- equals with string value';
SELECT id FROM tab WHERE data.key1 = 'the quick brown fox' ORDER BY id;

SELECT '-- equals with numeric value';
SELECT id FROM tab WHERE data.num = 42 ORDER BY id;

SELECT '-- like';
SELECT id FROM tab WHERE data.key1 LIKE '%quick%' ORDER BY id;

SELECT '-- startsWith';
SELECT id FROM tab WHERE startsWith(data.key1, 'lazy') ORDER BY id;

SELECT '-- endsWith';
SELECT id FROM tab WHERE endsWith(data.key1, 'fox') ORDER BY id;

SELECT '-- hasToken';
SELECT id FROM tab WHERE hasToken(data.key1, 'quick') ORDER BY id;

SELECT '-- Check that the text index is used for equals';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE data.key1 = 'the quick brown fox'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Check that the text index is used for numeric equals';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE data.num = 42
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Check that the text index is used for hasToken';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(data.key1, 'quick')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- CAST: hasAllTokens with ::String';
SELECT id FROM tab WHERE hasAllTokens(data.key1::String, 'the quick brown fox') ORDER BY id;

SELECT '-- CAST: equals with ::String';
SELECT id FROM tab WHERE data.key1::String = 'the quick brown fox' ORDER BY id;

SELECT '-- CAST: like with ::String';
SELECT id FROM tab WHERE data.key1::String LIKE '%quick%' ORDER BY id;

SELECT '-- CAST: startsWith with ::String';
SELECT id FROM tab WHERE startsWith(data.key1::String, 'lazy') ORDER BY id;

SELECT '-- CAST: hasToken with ::String';
SELECT id FROM tab WHERE hasToken(data.key1::String, 'quick') ORDER BY id;

SELECT '-- Check that the text index is used with CAST (hasAllTokens)';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(data.key1::String, 'the quick brown fox')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Check that the text index is used with CAST (equals)';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE data.key1::String = 'the quick brown fox'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Check that the text index is used with CAST (hasToken)';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(data.key1::String, 'quick')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Nested JSON subcolumns';

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    data JSON,
    INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (0, '{"a": {"b": "deep value one"}}');
INSERT INTO tab VALUES (1, '{"a": {"b": "deep value two"}}');
INSERT INTO tab VALUES (2, '{"a": {"b": "something else"}}');

SELECT id FROM tab WHERE data.a.b = 'deep value one' ORDER BY id;

SELECT '-- Check that the text index is used for nested subcolumn';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE data.a.b = 'deep value one'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;
