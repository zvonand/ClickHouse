#!/usr/bin/env bash
# Test _row_number virtual column for CSV format with and without parallel parsing

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

DATA_DIR=$USER_FILES_PATH/$CLICKHOUSE_TEST_UNIQUE_NAME
mkdir -p "$DATA_DIR"

# Create a CSV file
cat > "$DATA_DIR/data.csv" <<EOF
10,"x"
20,"y"
30,"z"
40,"w"
50,"v"
EOF

${CLICKHOUSE_CLIENT} --query "SELECT c1, _row_number FROM file('$DATA_DIR/data.csv', CSV, 'c1 UInt32, c2 String') ORDER BY c1 SETTINGS input_format_parallel_parsing = 0"
${CLICKHOUSE_CLIENT} --query "SELECT c1, _row_number FROM file('$DATA_DIR/data.csv', CSV, 'c1 UInt32, c2 String') ORDER BY c1 SETTINGS input_format_parallel_parsing = 1"

${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM file('$DATA_DIR/data.csv', CSV, 'c1 UInt32, c2 String') ORDER BY _row_number SETTINGS input_format_parallel_parsing = 0"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM file('$DATA_DIR/data.csv', CSV, 'c1 UInt32, c2 String') ORDER BY _row_number SETTINGS input_format_parallel_parsing = 1"

rm -rf "$DATA_DIR"
