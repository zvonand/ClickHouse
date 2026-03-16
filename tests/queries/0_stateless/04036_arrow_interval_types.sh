#!/usr/bin/env bash
# Tags: no-fasttest
# ^ uses the Arrow library, which is not available under fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that all interval kinds can be exported to Arrow/ArrowStream and values are preserved
for fmt in Arrow ArrowStream; do
    echo "=== $fmt ==="
    ${CLICKHOUSE_LOCAL} -q "
        SELECT
            3::IntervalNanosecond  AS ns,
            4::IntervalMicrosecond AS us,
            5::IntervalMillisecond AS ms,
            6::IntervalSecond      AS s,
            7::IntervalMinute      AS m,
            8::IntervalHour        AS h,
            9::IntervalDay         AS d,
            10::IntervalWeek       AS w,
            11::IntervalMonth      AS mo,
            12::IntervalQuarter    AS q,
            13::IntervalYear       AS y
        FORMAT $fmt
    " | ${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('-', '$fmt')"
done
