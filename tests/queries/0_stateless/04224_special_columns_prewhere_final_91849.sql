-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/91849
-- Special columns (`ver`, `is_deleted`, `sign`) used in `PREWHERE` on `FINAL`
-- were dropped from the read plan in `ReadFromMergeTree`, causing `NOT_FOUND_COLUMN_IN_BLOCK`.
-- The test only checks that the queries no longer throw an exception: the exact
-- result of `PREWHERE` on `FINAL` is not deterministic across all settings combinations
-- (it depends on whether `PREWHERE` is applied before or after the `FINAL` merge).

DROP TABLE IF EXISTS test_replacing_mt_91849;
CREATE TABLE test_replacing_mt_91849
(
    key Int64,
    someCol String,
    ver DateTime
) ENGINE = ReplacingMergeTree(ver) ORDER BY key;

INSERT INTO test_replacing_mt_91849 VALUES
    (1, 'test1', '2020-01-01'),
    (1, 'test2', '2021-01-01'),
    (2, 'test3', '2020-06-01'),
    (2, 'test4', '2021-06-01');

SELECT key, someCol FROM test_replacing_mt_91849 FINAL PREWHERE ver > '2020-01-01' ORDER BY key FORMAT Null;

DROP TABLE test_replacing_mt_91849;

DROP TABLE IF EXISTS test_replacing_mt_is_deleted_91849;
CREATE TABLE test_replacing_mt_is_deleted_91849
(
    key Int64,
    someCol String,
    ver UInt64,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key;

INSERT INTO test_replacing_mt_is_deleted_91849 VALUES
    (1, 'test1', 1, 0),
    (1, 'test2', 2, 0),
    (2, 'test3', 1, 0),
    (2, 'test4', 2, 1),
    (3, 'test5', 1, 1);

SELECT count() FROM test_replacing_mt_is_deleted_91849 FINAL PREWHERE ver > 0 AND is_deleted = 0 FORMAT Null;
SELECT count(ver) FROM test_replacing_mt_is_deleted_91849 FINAL PREWHERE is_deleted = 0 FORMAT Null;
SELECT key, someCol FROM test_replacing_mt_is_deleted_91849 FINAL PREWHERE ver > 0 AND is_deleted = 0 ORDER BY key FORMAT Null;

DROP TABLE test_replacing_mt_is_deleted_91849;

DROP TABLE IF EXISTS test_collapsing_mt_91849;
CREATE TABLE test_collapsing_mt_91849
(
    key Int64,
    someCol String,
    sign Int8
) ENGINE = CollapsingMergeTree(sign) ORDER BY key;

INSERT INTO test_collapsing_mt_91849 VALUES
    (1, 'test1',  1),
    (1, 'test1', -1),
    (2, 'test2',  1),
    (3, 'test3',  1);

SELECT key, someCol FROM test_collapsing_mt_91849 FINAL PREWHERE sign = 1 ORDER BY key FORMAT Null;

DROP TABLE test_collapsing_mt_91849;

DROP TABLE IF EXISTS test_versioned_collapsing_mt_91849;
CREATE TABLE test_versioned_collapsing_mt_91849
(
    key Int64,
    someCol String,
    sign Int8,
    ver UInt64
) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY key;

INSERT INTO test_versioned_collapsing_mt_91849 VALUES
    (1, 'test1',          1, 1),
    (1, 'test1',         -1, 1),
    (2, 'test2',          1, 2),
    (2, 'test2_updated', -1, 2),
    (2, 'test2_updated',  1, 3),
    (3, 'test3',          1, 1);

SELECT key, someCol FROM test_versioned_collapsing_mt_91849 FINAL PREWHERE sign = 1 AND ver > 1 ORDER BY key FORMAT Null;
SELECT ver FROM test_versioned_collapsing_mt_91849 FINAL PREWHERE sign = 1 ORDER BY key FORMAT Null;

DROP TABLE test_versioned_collapsing_mt_91849;

SELECT 'ok';
