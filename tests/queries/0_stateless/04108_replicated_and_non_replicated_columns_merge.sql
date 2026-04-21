SELECT * FROM (
	SELECT
		concat('chi_clickhouse_metric_', metric) AS name,
		CAST(value, 'Float64') AS value,
		description AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM merge('system', '^(metrics|custom_metrics)$')
	UNION ALL
	SELECT
		concat('chi_clickhouse_metric_', replaceAll(metric, '.', '_')) AS name,
		value,
		'' AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.asynchronous_metrics
	WHERE NOT match(metric, '(CPU|MHz_)\d+$') AND NOT match(metric, '^Block.+_(nvme|sd[a-z])')
	UNION ALL
	SELECT
		concat('chi_clickhouse_event_', event) AS name,
		CAST(value, 'Float64') AS value,
		description AS help,
		map('hostname', hostName()) AS labels,
		'counter' AS type
	FROM system.events
	UNION ALL
	SELECT
		concat('chi_clickhouse_metric_SystemErrors_', name) AS name,
		CAST(sum(value), 'Float64') AS value,
		'Error counter from system.errors' AS help,
		map('hostname', hostName()) AS labels,
		'counter' AS type
	FROM system.errors
	GROUP BY name
	UNION ALL
	SELECT
		'chi_clickhouse_metric_MemoryDictionaryBytesAllocated' AS name,
		CAST(sum(bytes_allocated), 'Float64') AS value,
		'Memory size allocated for dictionaries' AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.dictionaries
	UNION ALL
	SELECT
		'chi_clickhouse_metric_LongestRunningQuery' AS name,
		CAST(ifNull(max(elapsed), 0), 'Float64') AS value,
		'Longest running query time' AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.processes
	UNION ALL
	SELECT
		'chi_clickhouse_metric_ChangedSettingsHash' AS name,
		CAST(groupBitXor(cityHash64(name, value)), 'Float64') AS value,
		'Hash of changed settings for drift detection' AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.settings WHERE changed
	UNION ALL
	WITH
		['chi_clickhouse_table_partitions', 'chi_clickhouse_table_parts', 'chi_clickhouse_table_parts_bytes', 'chi_clickhouse_table_parts_bytes_uncompressed', 'chi_clickhouse_table_parts_rows'] AS names,
		[uniq(partition), count(), sum(bytes), sum(data_uncompressed_bytes), sum(rows)] AS values,
		arrayJoin(arrayZip(names, values)) AS tpl
	SELECT
		tpl.1 AS name,
		CAST(tpl.2, 'Float64') AS value,
		'' AS help,
		map('database', database, 'table', table, 'active', toString(active), 'hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.parts
	GROUP BY active, database, table
	UNION ALL
	SELECT
		'chi_clickhouse_metric_DiskDataBytes' AS name,
		CAST(sum(bytes_on_disk), 'Float64') AS value,
		'Total bytes on disk for all parts' AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.parts
	UNION ALL
	SELECT
		'chi_clickhouse_metric_MemoryPrimaryKeyBytesAllocated' AS name,
		CAST(sum(primary_key_bytes_in_memory_allocated), 'Float64') AS value,
		'Memory allocated for primary keys' AS help,
		map('hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.parts
	UNION ALL
	WITH
		['chi_clickhouse_table_mutations', 'chi_clickhouse_table_mutations_parts_to_do'] AS names,
		[CAST(count(), 'Float64'), CAST(sum(parts_to_do), 'Float64')] AS values,
		arrayJoin(arrayZip(names, values)) AS tpl
	SELECT
		tpl.1 AS name,
		tpl.2 AS value,
		'' AS help,
		map('database', database, 'table', table, 'hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.mutations
	WHERE is_done = 0
	GROUP BY database, table
	UNION ALL
	SELECT
		'chi_clickhouse_metric_DiskFreeBytes' AS name,
		CAST(free_space, 'Float64') AS value,
		'' AS help,
		map('disk', disk_name, 'hostname', hostName()) AS labels,
		'gauge' AS type
	FROM (SELECT name AS disk_name, free_space, total_space FROM system.disks WHERE type IN ('local', 'Local'))
	UNION ALL
	SELECT
		'chi_clickhouse_metric_DiskTotalBytes' AS name,
		CAST(total_space, 'Float64') AS value,
		'' AS help,
		map('disk', disk_name, 'hostname', hostName()) AS labels,
		'gauge' AS type
	FROM (SELECT name AS disk_name, free_space, total_space FROM system.disks WHERE type IN ('local', 'Local'))
	UNION ALL
	SELECT
		'chi_clickhouse_system_replicas_is_session_expired' AS name,
		CAST(is_session_expired, 'Float64') AS value,
		'Whether ZooKeeper session has expired' AS help,
		map('database', database, 'table', table, 'hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.replicas
	UNION ALL
	SELECT
		'chi_clickhouse_metric_DetachedParts' AS name,
		CAST(count(), 'Float64') AS value,
		'Count of currently detached data parts' AS help,
		map('database', database, 'table', table, 'disk', disk, 'reason', if(coalesce(reason, 'unknown') = '', 'detached_by_user', coalesce(reason, 'unknown')), 'hostname', hostName()) AS labels,
		'gauge' AS type
	FROM system.detached_parts
	GROUP BY database, table, disk, reason
) ORDER BY name ASC
FORMAT Null


