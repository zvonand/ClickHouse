ATTACH TABLE _ UUID 'aad105e8-0a39-4552-a549-d6999e03b39a'
(
    `hostname` LowCardinality(String) COMMENT 'Hostname of the server which is connected to or disconnected from ZooKeeper.',
    `type` Enum8('Connected' = 0, 'Disconnected' = 1) COMMENT 'The type of the event. Possible values: Connected, Disconnected.',
    `event_date` Date COMMENT 'Date of the entry.',
    `event_time` DateTime COMMENT 'Time of the entry',
    `event_time_microseconds` DateTime64(6) COMMENT 'Time of the entry with microseconds precision.',
    `name` String COMMENT 'ZooKeeper cluster\'s name.',
    `host` String COMMENT 'The hostname/IP of the ZooKeeper node that ClickHouse connected to or disconnected from.',
    `port` UInt16 COMMENT 'The port of the ZooKeeper node that ClickHouse connected to or disconnected from.',
    `index` UInt8 COMMENT 'The index of the ZooKeeper node that ClickHouse connected to or disconnected from. The index is from ZooKeeper config.',
    `client_id` Int64 COMMENT 'Session id of the connection.',
    `keeper_api_version` UInt8 COMMENT 'Keeper API version.',
    `enabled_feature_flags` Array(Enum16('FILTERED_LIST' = 0, 'MULTI_READ' = 1, 'CHECK_NOT_EXISTS' = 2, 'CREATE_IF_NOT_EXISTS' = 3, 'REMOVE_RECURSIVE' = 4, 'MULTI_WATCHES' = 5, 'CHECK_STAT' = 6, 'PERSISTENT_WATCHES' = 7, 'CREATE_WITH_STATS' = 8, 'TRY_REMOVE' = 9, 'LIST_WITH_STAT_AND_DATA' = 10)) COMMENT 'Feature flags which are enabled. Only applicable to ClickHouse Keeper.',
    `availability_zone` String COMMENT 'Availability zone',
    `reason` LowCardinality(String) COMMENT 'Reason for the connection or disconnection.',
    INDEX event_time_index event_time TYPE minmax GRANULARITY 1,
    INDEX event_time_microseconds_index event_time_microseconds TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
TTL event_date + toIntervalDay(7)
SETTINGS index_granularity = 8192
COMMENT 'Contains history of ZooKeeper connections.\n\nIt is safe to truncate or drop this table at any time.'
