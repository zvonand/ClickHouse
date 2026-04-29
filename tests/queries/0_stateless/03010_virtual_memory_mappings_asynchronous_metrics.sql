-- Tags: no-replicated-database, no-darwin
-- no-darwin: VMMaxMapCount and VMNumMaps are populated from /proc/sys/vm/max_map_count on Linux only.

SELECT least(value, 0) FROM system.asynchronous_metrics WHERE metric = 'VMMaxMapCount';
SELECT least(value, 0) FROM system.asynchronous_metrics WHERE metric = 'VMNumMaps';
