#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

name=test_01655_plan_optimizations_optimize_read_in_window_order

$CLICKHOUSE_CLIENT -nm -q "
drop table if exists ${name};
drop table if exists ${name}_n;
drop table if exists ${name}_n_x;
create table ${name} engine=MergeTree order by tuple() as select toInt64((sin(number)+2)*65535)%10 as n, number as x from numbers_mt(1000);
create table ${name}_n engine=MergeTree order by n as select * from ${name} order by n;
create table ${name}_n_x engine=MergeTree order by (n, x) as select * from ${name} order by n, x;
optimize table ${name}_n final;
optimize table ${name}_n_x final;
"

echo 'Partial sorting plan'
$CLICKHOUSE_CLIENT -nm -q "
SELECT '  optimize_read_in_window_order=0';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n SETTINGS optimize_read_in_order=0,optimize_read_in_window_order=0,enable_analyzer=0;
SELECT '  optimize_read_in_window_order=0, enable_analyzer=1';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n SETTINGS optimize_read_in_order=0,optimize_read_in_window_order=0,enable_analyzer=0;
SELECT '  optimize_read_in_window_order=1';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n SETTINGS optimize_read_in_order=1,enable_analyzer=0;
SELECT '  optimize_read_in_window_order=1, enable_analyzer=1';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n SETTINGS optimize_read_in_order=1,enable_analyzer=1;
" | grep -iE "sort description|optimize_read_in_window_order="

echo 'No sorting plan'
$CLICKHOUSE_CLIENT -nm -q "
SELECT '  optimize_read_in_window_order=0';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n_x SETTINGS optimize_read_in_order=0,optimize_read_in_window_order=0,enable_analyzer=0;
SELECT '  optimize_read_in_window_order=0, enable_analyzer=1';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n_x SETTINGS optimize_read_in_order=0,optimize_read_in_window_order=0,enable_analyzer=1;
SELECT '  optimize_read_in_window_order=1';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n_x SETTINGS optimize_read_in_order=1,enable_analyzer=0;
SELECT '  optimize_read_in_window_order=1, enable_analyzer=1';
explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n_x SETTINGS optimize_read_in_order=1,enable_analyzer=1;
" | grep -iE "sort description|optimize_read_in_window_order="

echo 'Complex ORDER BY'
$CLICKHOUSE_CLIENT -nm -q "
CREATE TABLE ${name}_complex (unique1 Int32, unique2 Int32, ten Int32) ENGINE=MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO ${name}_complex VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5);
SELECT '  optimize_read_in_window_order=0';
SELECT ten, sum(unique1) + sum(unique2) AS res, rank() OVER (ORDER BY sum(unique1) + sum(unique2) ASC) AS rank FROM ${name}_complex GROUP BY ten ORDER BY ten ASC SETTINGS optimize_read_in_order=0,optimize_read_in_window_order=0;
SELECT '  optimize_read_in_window_order=1';
SELECT ten, sum(unique1) + sum(unique2) AS res, rank() OVER (ORDER BY sum(unique1) + sum(unique2) ASC) AS rank FROM ${name}_complex GROUP BY ten ORDER BY ten ASC SETTINGS optimize_read_in_order=1;
"

$CLICKHOUSE_CLIENT -nm -q "
drop table ${name};
drop table ${name}_n;
drop table ${name}_n_x;
drop table ${name}_complex;
"
