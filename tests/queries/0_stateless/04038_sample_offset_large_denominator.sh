#!/usr/bin/env bash

# Regression test for AST round-trip of SAMPLE ratios.
# Verifies that formatting a SAMPLE clause and parsing it back produces identical SQL.
# The original bug: OFFSET 1.1920928955078125e-7 produces a denominator of 10^23,
# which overflowed UInt64 in ParserSampleRatio, turning the denominator into 0.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function check_roundtrip()
{
    local query="$1"
    local first second
    first=$($CLICKHOUSE_FORMAT --oneline --query "$query" 2>&1)
    second=$($CLICKHOUSE_FORMAT --oneline --query "$first" 2>&1)

    if [ "$first" = "$second" ]; then
        echo "OK"
    else
        echo "FAIL: AST round-trip mismatch for: $query"
        echo "First:  $first"
        echo "Second: $second"
    fi
}

# Original fuzzer crash: denominator 10^23 overflows UInt64.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1 / 10 OFFSET 1.1920928955078125e-7"

# Scientific notation with large negative exponent (denominator 10^20).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1e-20"

# Decimal with 25 digits after point — denominator is 10^25.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 0.0000000000000000000000001"

# Both SAMPLE and OFFSET have large denominators.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1e-10 OFFSET 1e-12"

# Explicit rational with numerator and denominator exceeding UInt64 max.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 99999999999999999999 / 100000000000000000001"

# Boundary: 20-digit denominator (10^19 fits in UInt64, 10^20 does not).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1 / 100000000000000000000"

# Boundary: 19-digit denominator (10^18, fits in UInt64 — should always have worked).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1 / 1000000000000000000"

# Large numerator in scientific notation.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 9999999999999999.0e-30"

# Many significant digits in the decimal part.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 0.123456789012345678901234567890"

# Fractional SAMPLE with fractional OFFSET (both as decimals).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 0.00000001 OFFSET 0.99999999"

# SAMPLE in a subquery inside a JOIN (mirrors the original fuzzer-found pattern).
check_roundtrip "SELECT * FROM numbers(1) JOIN (SELECT number FROM numbers(1) SAMPLE 1 / 10 OFFSET 1e-8) AS sub ON sub.number = number"
