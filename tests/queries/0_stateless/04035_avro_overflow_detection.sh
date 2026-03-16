#!/usr/bin/env bash
# Tags: no-fasttest

# Test that Avro format properly detects numeric overflow and type conversion errors

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

#
# Integer overflow tests
#

echo "Test 1: Long to UInt8 overflow"
${CLICKHOUSE_LOCAL} -q "SELECT toInt64(300) AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x UInt8' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 2: Negative to UInt8 overflow"
${CLICKHOUSE_LOCAL} -q "SELECT toInt64(-1) AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x UInt8' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 3: Large Int64 to Int8 overflow"
${CLICKHOUSE_LOCAL} -q "SELECT toInt64(200) AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int8' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 4: Negative Int64 to UInt64 overflow"
${CLICKHOUSE_LOCAL} -q "SELECT toInt64(-100) AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x UInt64' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 5: Value within range (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT toInt64(100) AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x UInt8' -q "SELECT * FROM table"

#
# Float to float overflow tests
#

echo "Test 6: Large double to Float32 overflow"
${CLICKHOUSE_LOCAL} -q "SELECT toFloat64(1e300) AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Float32' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

#
# Float to integer conversion tests
#

echo "Test 7: Fractional float to int (truncates)"
${CLICKHOUSE_LOCAL} -q "SELECT 1.5::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table"

echo "Test 8: Negative fractional float to int (truncates)"
${CLICKHOUSE_LOCAL} -q "SELECT -2.7::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table"

echo "Test 9: Large float outside int range"
${CLICKHOUSE_LOCAL} -q "SELECT 1e20::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 10: Whole number float to int (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 100.0::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table"

echo "Test 11: Zero float to int (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 0.0::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table"

echo "Test 12: Negative whole float to int (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT -50.0::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table"

echo "Test 13: NaN to int (converts to INT32_MIN)"
${CLICKHOUSE_LOCAL} -q "SELECT nan::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table"

echo "Test 14: Infinity to int"
${CLICKHOUSE_LOCAL} -q "SELECT inf::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Int32' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

#
# Bool conversion tests
#

echo "Test 15: Int 1 to Bool (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 1::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Bool' -q "SELECT * FROM table"

echo "Test 16: Int 0 to Bool (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 0::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Bool' -q "SELECT * FROM table"

echo "Test 17: Int 2 to Bool (converts to true)"
${CLICKHOUSE_LOCAL} -q "SELECT 2::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Bool' -q "SELECT * FROM table"

echo "Test 18: Negative int to Bool"
${CLICKHOUSE_LOCAL} -q "SELECT -1::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x Bool' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

#
# IPv4 conversion tests
#

echo "Test 19: Negative to IPv4"
${CLICKHOUSE_LOCAL} -q "SELECT -1::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x IPv4' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 20: Value exceeding UInt32 max to IPv4"
${CLICKHOUSE_LOCAL} -q "SELECT 5000000000::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x IPv4' -q "SELECT * FROM table" 2>&1 | \
    grep -q "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" && echo "OK" || echo "FAIL"

echo "Test 21: Fractional float to IPv4 (truncates)"
${CLICKHOUSE_LOCAL} -q "SELECT 192.5::Float64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x IPv4' -q "SELECT * FROM table"

echo "Test 22: Valid UInt32 to IPv4 (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 3232235777::UInt64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x IPv4' -q "SELECT * FROM table"

echo "Test 23: Zero to IPv4 (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 0::Int64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x IPv4' -q "SELECT * FROM table"

echo "Test 24: Max IPv4 (should succeed)"
${CLICKHOUSE_LOCAL} -q "SELECT 4294967295::UInt64 AS x FORMAT Avro" | \
    ${CLICKHOUSE_LOCAL} --input-format Avro -S 'x IPv4' -q "SELECT * FROM table"
