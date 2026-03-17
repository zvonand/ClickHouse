ATTACH TABLE _ UUID '0093a6e8-a24f-491d-bcfa-6c13e937c3a0'
(
    `hostname` LowCardinality(String) COMMENT 'Hostname of the server.',
    `event_date` Date COMMENT 'Date the group was flushed.',
    `event_time` DateTime COMMENT 'Time the group was flushed.',
    `session_id` Int64 COMMENT 'Session id.',
    `parent_path` String COMMENT 'Prefix of the path.',
    `operation` Enum16('Close' = -11, 'Error' = -1, 'Watch' = 0, 'Create' = 1, 'Remove' = 2, 'Exists' = 3, 'Get' = 4, 'Set' = 5, 'GetACL' = 6, 'SetACL' = 7, 'SimpleList' = 8, 'Sync' = 9, 'Heartbeat' = 11, 'List' = 12, 'Check' = 13, 'Multi' = 14, 'Create2' = 15, 'Reconfig' = 16, 'CheckWatch' = 17, 'RemoveWatch' = 18, 'MultiRead' = 22, 'Auth' = 100, 'SetWatch' = 101, 'SetWatch2' = 105, 'AddWatch' = 106, 'FilteredList' = 500, 'CheckNotExists' = 501, 'CreateIfNotExists' = 502, 'RemoveRecursive' = 503, 'CheckStat' = 504, 'TryRemove' = 505, 'FilteredListWithStatsAndData' = 506, 'SessionID' = 997) COMMENT 'Type of ZooKeeper operation.',
    `count` UInt32 COMMENT 'Number of operations in the (session_id, parent_path, operation) group.',
    `errors` Map(Enum8('ZNOWATCHER' = -121, 'ZNOTREADONLY' = -119, 'ZSESSIONMOVED' = -118, 'ZNOTHING' = -117, 'ZCLOSING' = -116, 'ZAUTHFAILED' = -115, 'ZINVALIDACL' = -114, 'ZINVALIDCALLBACK' = -113, 'ZSESSIONEXPIRED' = -112, 'ZNOTEMPTY' = -111, 'ZNODEEXISTS' = -110, 'ZNOCHILDRENFOREPHEMERALS' = -108, 'ZBADVERSION' = -103, 'ZNOAUTH' = -102, 'ZNONODE' = -101, 'ZAPIERROR' = -100, 'ZOUTOFMEMORY' = -10, 'ZINVALIDSTATE' = -9, 'ZBADARGUMENTS' = -8, 'ZOPERATIONTIMEOUT' = -7, 'ZUNIMPLEMENTED' = -6, 'ZMARSHALLINGERROR' = -5, 'ZCONNECTIONLOSS' = -4, 'ZDATAINCONSISTENCY' = -3, 'ZRUNTIMEINCONSISTENCY' = -2, 'ZSYSTEMERROR' = -1, 'ZOK' = 0), UInt32) COMMENT 'Errors in the (session_id, parent_path, operation) group.',
    `average_latency` Float64 COMMENT 'Average latency across all operations in (session_id, parent_path, operation) group, in microseconds.',
    `component` LowCardinality(String) COMMENT 'Component that caused the event.',
    INDEX event_time_index event_time TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
TTL event_date + toIntervalDay(30)
SETTINGS index_granularity = 8192
COMMENT 'Contains statistics (number of operations, latencies, errors) of ZooKeeper operations grouped by session_id, parent_path and operation. Periodically flushed to disk.\n\nIt is safe to truncate or drop this table at any time.'
