-- Tags: replica

-- insert_deduplication_token should work for INSERT SELECT without ORDER BY ALL

DROP TABLE IF EXISTS t_dedup_token_insert_select SYNC;

CREATE TABLE t_dedup_token_insert_select (k UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_dedup_token_insert_select', '1')
ORDER BY k;

SELECT 'insert_deduplication_token with INSERT SELECT without ORDER BY ALL';
INSERT INTO t_dedup_token_insert_select SELECT number FROM numbers(100) SETTINGS insert_deduplication_token = 'token1';
INSERT INTO t_dedup_token_insert_select SELECT number FROM numbers(100) SETTINGS insert_deduplication_token = 'token1';
SELECT count() FROM t_dedup_token_insert_select;

SELECT 'different token should insert new data';
INSERT INTO t_dedup_token_insert_select SELECT number FROM numbers(100) SETTINGS insert_deduplication_token = 'token2';
SELECT count() FROM t_dedup_token_insert_select;

SELECT 'same token again should deduplicate';
INSERT INTO t_dedup_token_insert_select SELECT number FROM numbers(100) SETTINGS insert_deduplication_token = 'token2';
SELECT count() FROM t_dedup_token_insert_select;

TRUNCATE TABLE t_dedup_token_insert_select;

SELECT 'insert_deduplication_token with INSERT SELECT with ORDER BY ALL still works';
INSERT INTO t_dedup_token_insert_select SELECT number FROM numbers(100) ORDER BY ALL SETTINGS insert_deduplication_token = 'token3';
INSERT INTO t_dedup_token_insert_select SELECT number FROM numbers(100) ORDER BY ALL SETTINGS insert_deduplication_token = 'token3';
SELECT count() FROM t_dedup_token_insert_select;

DROP TABLE t_dedup_token_insert_select SYNC;
