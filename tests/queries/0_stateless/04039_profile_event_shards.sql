-- Tags: shard

-- Single distributed query: 2 shards
SELECT count() FROM remote('127.0.0.{1,2}', system.one) FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'SELECT count() FROM remote(''127.0.0.{1,2}''%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Two distributed table functions in one query: 2 + 3 = 5 shards
SELECT count() FROM remote('127.0.0.{1,2}', system.one) AS t1 CROSS JOIN remote('127.0.0.{1,2,3}', system.one) AS t2 FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'SELECT count() FROM remote(''127.0.0.{1,2}''%) AS t1 CROSS JOIN remote(''127.0.0.{1,2,3}''%) AS t2%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
