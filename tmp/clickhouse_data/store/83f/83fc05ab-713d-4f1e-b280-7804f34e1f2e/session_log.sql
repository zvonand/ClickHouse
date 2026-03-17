ATTACH TABLE _ UUID '4479240c-4f95-41b8-90e0-03edabc09ff9'
(
    `hostname` LowCardinality(String) COMMENT 'Hostname of the server executing the query.',
    `type` Enum8('LoginFailure' = 0, 'LoginSuccess' = 1, 'Logout' = 2) COMMENT 'Login/logout result. Possible values: LoginFailure — Login error. LoginSuccess — Successful login. Logout — Logout from the system.',
    `auth_id` UUID COMMENT 'Authentication ID, which is a UUID that is automatically generated each time user logins.',
    `session_id` String COMMENT 'Session ID that is passed by client via HTTP interface.',
    `event_date` Date COMMENT 'Login/logout date.',
    `event_time` DateTime COMMENT 'Login/logout time.',
    `event_time_microseconds` DateTime64(6) COMMENT 'Login/logout starting time with microseconds precision.',
    `user` Nullable(String) COMMENT 'User name.',
    `auth_type` Nullable(Enum8('NO_PASSWORD' = 0, 'PLAINTEXT_PASSWORD' = 1, 'SHA256_PASSWORD' = 2, 'DOUBLE_SHA1_PASSWORD' = 3, 'LDAP' = 4, 'KERBEROS' = 5, 'SSL_CERTIFICATE' = 6, 'BCRYPT_PASSWORD' = 7, 'SSH_KEY' = 8, 'HTTP' = 9, 'JWT' = 10, 'SCRAM_SHA256_PASSWORD' = 11, 'NO_AUTHENTICATION' = 12)) COMMENT 'The authentication type.',
    `profiles` Array(LowCardinality(String)) COMMENT 'The list of profiles set for all roles and/or users.',
    `roles` Array(LowCardinality(String)) COMMENT 'The list of roles to which the profile is applied.',
    `settings` Array(Tuple(LowCardinality(String), String)) COMMENT 'Settings that were changed when the client logged in/out.',
    `client_address` IPv6 COMMENT 'The IP address that was used to log in/out.',
    `client_port` UInt16 COMMENT 'The client port that was used to log in/out.',
    `interface` Enum8('TCP' = 1, 'HTTP' = 2, 'gRPC' = 3, 'MySQL' = 4, 'PostgreSQL' = 5, 'Local' = 6, 'TCP_Interserver' = 7, 'Prometheus' = 8, 'Background' = 9) COMMENT 'The interface from which the login was initiated.',
    `client_hostname` String COMMENT 'The hostname of the client machine where the clickhouse-client or another TCP client is run.',
    `client_name` String COMMENT 'The clickhouse-client or another TCP client name.',
    `client_revision` UInt32 COMMENT 'Revision of the clickhouse-client or another TCP client.',
    `client_version_major` UInt32 COMMENT 'The major version of the clickhouse-client or another TCP client.',
    `client_version_minor` UInt32 COMMENT 'The minor version of the clickhouse-client or another TCP client.',
    `client_version_patch` UInt32 COMMENT 'Patch component of the clickhouse-client or another TCP client version.',
    `failure_reason` String COMMENT 'The exception message containing the reason for the login/logout failure.',
    INDEX event_time_index event_time TYPE minmax GRANULARITY 1,
    INDEX event_time_microseconds_index event_time_microseconds TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192
COMMENT 'Contains information about all successful and failed login and logout events.\n\nIt is safe to truncate or drop this table at any time.'
