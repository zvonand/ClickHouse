#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-replicated-database, no-parallel, no-shared-merge-tree
# Regression test for https://github.com/ClickHouse/ClickHouse/pull/92141
# (STID 3547-447e):
#
# When a part on disk has `txn_version.txt.tmp` but no `txn_version.txt` (an
# incomplete write that was not atomically renamed), `VersionMetadataOnDisk::loadMetadata`
# returns a `VersionInfo` with `creation_tid = Tx::DummyTID` and
# `creation_csn = Tx::RolledBackCSN`. `DummyTID` has `start_csn == NonTransactionalCSN`
# but `local_tid == DummyLocalTID`, which must not trip the assertion inside
# `TransactionID::isNonTransactional` called by `VersionMetadata::validateInfo` and
# `VersionInfo::wasInvolvedInTransaction`. Before the fix, the server aborted
# with signal 6 during part loading in debug and sanitizer builds.
#
# This test creates a part with no `txn_version.txt` (deferred persist is the
# default for non-transactional inserts), drops a bogus `txn_version.txt.tmp`
# into the part directory, and runs `DETACH` + `ATTACH`. The server must stay
# alive and the rolled-back part must be invisible.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_txn_tmp_leftover;
    CREATE TABLE t_txn_tmp_leftover (n Int64) ENGINE = MergeTree ORDER BY n;
    SYSTEM STOP MERGES t_txn_tmp_leftover;
    INSERT INTO t_txn_tmp_leftover VALUES (42);
"

PART_PATH=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_txn_tmp_leftover' AND active
    LIMIT 1
")

if [[ -z "${PART_PATH}" ]]; then
    echo "FAIL: could not locate active part for t_txn_tmp_leftover"
    exit 1
fi

# Simulate an incomplete write: drop a bogus `txn_version.txt.tmp` file into the
# part directory while keeping `txn_version.txt` absent.
echo "incomplete" > "${PART_PATH}/txn_version.txt.tmp"

# Trigger part reload. Before the fix this aborted the server with signal 6.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_txn_tmp_leftover"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_txn_tmp_leftover"

# Server must still be alive. The rolled-back part is marked `Outdated` by
# `MergeTreeData::loadDataPart` (creation_csn == RolledBackCSN branch) so the
# table is now empty.
${CLICKHOUSE_CLIENT} -q "SELECT 'select_ok', count() FROM t_txn_tmp_leftover"

# The rolled-back part still appears in system.parts as inactive with
# DummyTID / RolledBackCSN. Its visibility flag is exactly what we expect.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        'rolled_back_part',
        active,
        creation_tid = (1, 2, '00000000-0000-0000-0000-000000000000') AS is_dummy_tid,
        creation_csn = 18446744073709551615                               AS is_rolled_back_csn
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_txn_tmp_leftover'
        AND creation_csn = 18446744073709551615
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_txn_tmp_leftover"
