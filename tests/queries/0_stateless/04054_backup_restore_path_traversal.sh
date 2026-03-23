#!/usr/bin/env bash
# Test that RESTORE rejects backup entries with path traversal sequences (../)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tbl_backup_traversal (id UInt64, data String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl_backup_traversal VALUES (1, 'hello')"

backups_disk_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'" 2>/dev/null)

extra_content="EXTRA_FILE_CONTENT_HERE"
extra_size=${#extra_content}
extra_checksum=$(echo -n "${extra_content}" | md5sum | awk '{print $1}')
extra_data_path="data/default/tbl_backup_traversal/extra_payload.bin"

# Creates a backup, injects an extra file entry into its .backup metadata, and
# attempts to restore. Expects INSECURE_PATH rejection.
#   $1 - backup suffix
#   $2 - injected <name> value
#   $3 - (optional) injected <data_file> value; defaults to extra_data_path
inject_and_restore() {
    local suffix="$1"
    local injected_name="$2"
    local injected_data_file="${3:-${extra_data_path}}"
    local bname="${CLICKHOUSE_TEST_UNIQUE_NAME}_${suffix}"

    ${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl_backup_traversal TO Disk('backups', '${bname}')" > /dev/null 2>&1

    local bpath="${backups_disk_root}/${bname}"
    mkdir -p "${bpath}/$(dirname "${extra_data_path}")"
    echo -n "${extra_content}" > "${bpath}/${extra_data_path}"

    sed -i "s|</contents>|<file><name>${injected_name}</name><size>${extra_size}</size><checksum>${extra_checksum}</checksum><data_file>${injected_data_file}</data_file></file></contents>|" "${bpath}/.backup"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal"
    ${CLICKHOUSE_CLIENT} -m -q "RESTORE TABLE tbl_backup_traversal FROM Disk('backups', '${bname}'); -- { serverError INSECURE_PATH }"
}

# Helper to recreate the table between tests.
recreate_table() {
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS tbl_backup_traversal (id UInt64, data String) ENGINE = MergeTree ORDER BY id"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl_backup_traversal VALUES (1, 'hello')"
}

# Test 1: relative path traversal in <name>.
inject_and_restore "rel" "data/default/tbl_backup_traversal/all_0_0_0/../../../../../../../tmp/backup_traversal_test_output.txt"

# Verify the file was NOT written outside the backup directory.
if [ -f "/tmp/backup_traversal_test_output.txt" ]; then
    echo "FAIL: file written to /tmp/"
    rm -f "/tmp/backup_traversal_test_output.txt"
else
    echo "OK: path traversal was blocked"
fi

# Test 2: absolute path in <name>.
recreate_table
inject_and_restore "abs" "/tmp/backup_absolute_path_test_output.xml"

# Test 3: path traversal in <data_file> (source path for reading from the backup).
recreate_table
inject_and_restore "datafile" "data/default/tbl_backup_traversal/extra_payload.bin" "data/default/tbl_backup_traversal/all_0_0_0/../../../../../../../etc/passwd"

# Clean up.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal"
