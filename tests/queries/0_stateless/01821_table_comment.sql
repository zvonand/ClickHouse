-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1
(
    `n` Int8
)
ENGINE = Memory
COMMENT 'this is a temporary table';

CREATE TABLE t2
(
    `n` Int8
)
ENGINE = MergeTree
ORDER BY n
COMMENT 'this is a MergeTree table';

CREATE TABLE t3
(
    `n` Int8
)
ENGINE = Log
COMMENT 'this is a Log table';

CREATE TABLE t4
(
    `n` Int8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:10000',
    kafka_topic_list = 'test',
    kafka_group_name = 'test',
    kafka_format = 'JSONEachRow'
COMMENT 'this is a Kafka table';

CREATE TABLE t5
(
    `n` Int8
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY n
COMMENT 'this is a EmbeddedRocksDB table';

CREATE TABLE t6
(
    `n` Int8
)
ENGINE = Executable('script.py', TabSeparated)
COMMENT 'this is a Executable table';

SELECT
    name,
    comment
FROM system.tables
WHERE name IN ('t1', 't2', 't3', 't4', 't5', 't6')
    AND database = currentDatabase() order by name;

SHOW CREATE TABLE t1;

DROP TABLE t1, t2, t3, t4, t5, t6;
