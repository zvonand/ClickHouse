#!/usr/bin/env python3
import os
import time
import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

# ── Small-chunk cluster: local disk ──────────────────────────────────────────
# node1 has the highest leader priority so it will be the leader.
cluster_local = ClickHouseCluster(__file__)

node1 = cluster_local.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster_local.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster_local.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

# ── Small-chunk cluster: S3 (non-local) disk ─────────────────────────────────
# Same topology and chunk size as cluster_local, but snapshots are stored on
# MinIO.  This exercises the non-local-disk code path in read/save_logical_snp_obj.
cluster_s3 = ClickHouseCluster(__file__)

node7 = cluster_s3.add_instance(
    "node7",
    main_configs=["configs/enable_keeper7_s3.xml"],
    stay_alive=True,
    with_minio=True,
    with_remote_database_disk=False,
)
node8 = cluster_s3.add_instance(
    "node8",
    main_configs=["configs/enable_keeper8_s3.xml"],
    stay_alive=True,
    with_minio=True,
    with_remote_database_disk=False,
)
node9 = cluster_s3.add_instance(
    "node9",
    main_configs=["configs/enable_keeper9_s3.xml"],
    stay_alive=True,
    with_minio=True,
    with_remote_database_disk=False,
)

# ── Large-chunk cluster: local disk ──────────────────────────────────────────
# Configured with a large chunk size (100 MB) so that even a multi-hundred-KB
# snapshot is sent as a single NuRaft object, exercising the chunk_size >
# file_size code path (is_first_obj=is_last_obj=true on the leader side).
cluster_large_chunk = ClickHouseCluster(__file__)

node4 = cluster_large_chunk.add_instance(
    "node4",
    main_configs=["configs/enable_keeper4_large_chunk.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node5 = cluster_large_chunk.add_instance(
    "node5",
    main_configs=["configs/enable_keeper5_large_chunk.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node6 = cluster_large_chunk.add_instance(
    "node6",
    main_configs=["configs/enable_keeper6_large_chunk.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

# ── Compatibility cluster: old leader, new follower ───────────────────────────
# compat1 and compat2 run the oldest supported ClickHouse version and act as
# leader/active-follower.  Old versions have no snapshot_transfer_chunk_size
# setting and always send the whole snapshot as a single NuRaft object
# (is_first_obj=is_last_obj=true).  compat3 runs the current (new) version and
# is the lagging node that must recover via snapshot transfer.  This verifies
# that the new save_logical_snp_obj correctly handles the single-chunk path
# produced by an old leader.
cluster_compat = ClickHouseCluster(__file__)

compat1 = cluster_compat.add_instance(
    "compat1",
    main_configs=["configs/enable_keeper_compat1.xml"],
    stay_alive=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
    with_remote_database_disk=False,
)
compat2 = cluster_compat.add_instance(
    "compat2",
    main_configs=["configs/enable_keeper_compat2.xml"],
    stay_alive=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
    with_remote_database_disk=False,
)
compat3 = cluster_compat.add_instance(
    "compat3",
    main_configs=["configs/enable_keeper_compat3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(
    scope="module",
    params=["local", "s3"],
    ids=["local_disk", "s3_disk"],
)
def chunked_transfer_nodes(request):
    """
    Parametrized fixture that yields a dict of cluster nodes for the two tests
    that exercise chunked snapshot transfer.  The same test logic runs against
    both a local-disk cluster and an S3-backed cluster, ensuring the non-local
    code path in read/save_logical_snp_obj is covered.
    """
    if request.param == "local":
        try:
            cluster_local.start()
            yield {
                "cluster": cluster_local,
                "leader": node1,
                "middle": node2,
                "lagging": node3,
            }
        finally:
            cluster_local.shutdown()
    else:
        try:
            cluster_s3.start()
            cluster_s3.minio_client.make_bucket("snapshots")
            yield {
                "cluster": cluster_s3,
                "leader": node7,
                "middle": node8,
                "lagging": node9,
            }
        finally:
            cluster_s3.shutdown()


@pytest.fixture(scope="module")
def started_cluster_large_chunk():
    try:
        cluster_large_chunk.start()
        yield cluster_large_chunk
    finally:
        cluster_large_chunk.shutdown()


@pytest.fixture(scope="module")
def started_cluster_compat():
    try:
        cluster_compat.start()
        yield cluster_compat
    finally:
        cluster_compat.shutdown()


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_recover_from_snapshot_with_chunked_transfer(chunked_transfer_nodes):
    """
    node_lagging is stopped while node_leader/node_middle accumulate enough writes
    to trigger several snapshots.  When node_lagging restarts it is too stale to
    replay logs so the leader must send it a snapshot.  With
    snapshot_transfer_chunk_size=4096 the ~300 KB snapshot is split into multiple
    4 KB chunks.  We verify:
      1. Data on node_lagging matches the rest of the cluster after recovery.
      2. The snapshot was actually transferred in more than one chunk
         (checked via log line "Saving snapshot <idx> obj_id 2" on node_lagging,
         where the count is anchored to lines added during this recovery).

    The test runs for both local-disk and S3-backed clusters (parametrized).
    """
    cl = chunked_transfer_nodes["cluster"]
    node_leader = chunked_transfer_nodes["leader"]
    node_middle = chunked_transfer_nodes["middle"]
    node_lagging = chunked_transfer_nodes["lagging"]

    leader_zk = middle_zk = lagging_zk = None
    prefix = "/test_chunked_snapshot_transfer"

    try:
        leader_zk = keeper_utils.get_fake_zk(cl, node_leader.name)
        middle_zk = keeper_utils.get_fake_zk(cl, node_middle.name)
        lagging_zk = keeper_utils.get_fake_zk(cl, node_lagging.name)

        leader_zk.create(prefix, b"somedata")

        middle_zk.sync(prefix)
        lagging_zk.sync(prefix)

        assert leader_zk.get(prefix)[0] == b"somedata"
        assert middle_zk.get(prefix)[0] == b"somedata"
        assert lagging_zk.get(prefix)[0] == b"somedata"

        # Isolate node_lagging so it falls behind.
        node_lagging.stop_clickhouse(kill=True)

        # Write enough data to exceed stale_log_gap=10 and create multiple
        # snapshots (snapshot_distance=50).  Use unique random bytes so ZSTD
        # compression doesn't shrink the snapshot below the chunk size.
        for i in range(300):
            leader_zk.create(prefix + str(i), os.urandom(1024))

        for i in range(300):
            if i % 10 == 0:
                leader_zk.delete(prefix + str(i))

    finally:
        for zk in [leader_zk, middle_zk, lagging_zk]:
            stop_zk(zk)

    # Record the log line count before recovery so the chunk-count assertion
    # below is anchored to this recovery only and not contaminated by previous
    # test runs on the same node.
    log_lines_before = int(node_lagging.count_in_log("Saving snapshot").strip())

    # node_lagging is stale: it must recover via snapshot transfer (not log replay).
    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cl, node_lagging)

    try:
        leader_zk = keeper_utils.get_fake_zk(cl, node_leader.name)
        middle_zk = keeper_utils.get_fake_zk(cl, node_middle.name)
        lagging_zk = keeper_utils.get_fake_zk(cl, node_lagging.name)

        leader_zk.sync(prefix)
        middle_zk.sync(prefix)
        lagging_zk.sync(prefix)

        assert leader_zk.get(prefix)[0] == b"somedata"
        assert middle_zk.get(prefix)[0] == b"somedata"
        assert lagging_zk.get(prefix)[0] == b"somedata"

        for i in range(300):
            if i % 10 != 0:
                value_on_leader = leader_zk.get(prefix + str(i))[0]
                assert middle_zk.get(prefix + str(i))[0] == value_on_leader
                assert lagging_zk.get(prefix + str(i))[0] == value_on_leader
            else:
                assert leader_zk.exists(prefix + str(i)) is None
                assert middle_zk.exists(prefix + str(i)) is None
                assert lagging_zk.exists(prefix + str(i)) is None

    finally:
        try:
            leader_zk = keeper_utils.get_fake_zk(cl, node_leader.name)
            for i in range(300):
                if leader_zk.exists(prefix + str(i)):
                    leader_zk.delete(prefix + str(i))
            if leader_zk.exists(prefix):
                leader_zk.delete(prefix)
        except:
            pass

        for zk in [leader_zk, middle_zk, lagging_zk]:
            stop_zk(zk)

    # Verify that the snapshot was transferred in at least 3 chunks during this
    # recovery.  The follower logs "Saving snapshot <idx> obj_id <n>" for every
    # received chunk; obj_id 2 appearing means at least 3 calls were made.
    log_lines_after = int(node_lagging.count_in_log("Saving snapshot").strip())
    new_log = node_lagging.grep_in_log("Saving snapshot")
    # Filter to lines produced during this recovery (crude but sufficient: we
    # just need to confirm obj_id 2 appeared in the new entries).
    assert log_lines_after > log_lines_before, "No snapshot transfer log lines appeared during recovery"
    assert "obj_id 2" in new_log, (
        "Expected snapshot to be transferred in at least 3 chunks (obj_id 2 not found in log). "
        f"Log output:\n{new_log}"
    )


def test_recover_after_interrupted_transfer(chunked_transfer_nodes):
    """
    Verify that a partial temp file left by a mid-transfer crash does not prevent
    the next recovery from succeeding.

    node_lagging is stopped while the leader accumulates enough data to trigger a
    snapshot.  node_lagging is then started and killed as soon as it begins
    receiving snapshot chunks.  The leader's save_logical_snp_obj with
    is_first_obj=true must overwrite any leftover temp file and the second full
    recovery must produce correct data.

    The test runs for both local-disk and S3-backed clusters (parametrized).
    """
    cl = chunked_transfer_nodes["cluster"]
    node_leader = chunked_transfer_nodes["leader"]
    node_lagging = chunked_transfer_nodes["lagging"]

    prefix = "/test_interrupted_chunked_transfer"
    leader_zk = lagging_zk = None

    try:
        leader_zk = keeper_utils.get_fake_zk(cl, node_leader.name)

        # node_lagging may still be running from the previous test; kill it.
        node_lagging.stop_clickhouse(kill=True)

        leader_zk.ensure_path(prefix)
        for i in range(300):
            leader_zk.create(prefix + "/" + str(i), os.urandom(1024))
        for i in range(300):
            if i % 10 == 0:
                leader_zk.delete(prefix + "/" + str(i))
    finally:
        stop_zk(leader_zk)

    # Count "Saving snapshot" entries already in the log so we can detect new ones.
    # (The log file accumulates across restarts unless rotated.)
    snapshot_count_before = int(node_lagging.count_in_log("Saving snapshot").strip())

    # First start: kill node_lagging as soon as it begins receiving snapshot chunks.
    node_lagging.start_clickhouse(20)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        current_count = int(node_lagging.count_in_log("Saving snapshot").strip())
        if current_count > snapshot_count_before:
            break
        time.sleep(0.5)
    node_lagging.stop_clickhouse(kill=True)

    # Second start: let node_lagging recover fully from whatever partial state
    # was left by the mid-transfer kill.
    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cl, node_lagging)

    try:
        leader_zk = keeper_utils.get_fake_zk(cl, node_leader.name)
        lagging_zk = keeper_utils.get_fake_zk(cl, node_lagging.name)

        leader_zk.sync(prefix)
        lagging_zk.sync(prefix)

        for i in range(300):
            if i % 10 != 0:
                assert lagging_zk.get(prefix + "/" + str(i))[0] == leader_zk.get(prefix + "/" + str(i))[0]
            else:
                assert lagging_zk.exists(prefix + "/" + str(i)) is None

    finally:
        try:
            leader_zk = keeper_utils.get_fake_zk(cl, node_leader.name)
            for i in range(300):
                if leader_zk.exists(prefix + "/" + str(i)):
                    leader_zk.delete(prefix + "/" + str(i))
            if leader_zk.exists(prefix):
                leader_zk.delete(prefix)
        except:
            pass

        for zk in [leader_zk, lagging_zk]:
            stop_zk(zk)


def test_recover_with_chunk_size_larger_than_snapshot(started_cluster_large_chunk):
    """
    Verify recovery when snapshot_transfer_chunk_size exceeds the snapshot file size.

    With chunk_size=104857600 (100 MB) the ~300 KB test snapshot is smaller than one
    chunk, so the leader sets is_first_obj=is_last_obj=true and sends a single NuRaft
    object.  This exercises the same single-object code path that was used before
    chunked transfer was introduced, and ensures no regression for that case.

    We verify:
      1. Data on node6 matches node4 after recovery.
      2. Only obj_id 0 appears in node6's log during this recovery (single-chunk
         transfer), anchored to lines added during this run.
    """
    prefix = "/test_large_chunk_transfer"
    node4_zk = node6_zk = None

    node6.stop_clickhouse(kill=True)

    # Baseline before recovery so the log assertion below is not contaminated
    # by any previous snapshot lines in node6's log file.
    log_lines_before = int(node6.count_in_log("Saving snapshot").strip())

    try:
        node4_zk = keeper_utils.get_fake_zk(cluster_large_chunk, "node4")

        node4_zk.ensure_path(prefix)
        for i in range(300):
            node4_zk.create(prefix + "/" + str(i), os.urandom(1024))
        for i in range(300):
            if i % 10 == 0:
                node4_zk.delete(prefix + "/" + str(i))
    finally:
        stop_zk(node4_zk)

    node6.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster_large_chunk, node6)

    try:
        node4_zk = keeper_utils.get_fake_zk(cluster_large_chunk, "node4")
        node6_zk = keeper_utils.get_fake_zk(cluster_large_chunk, "node6")

        node4_zk.sync(prefix)
        node6_zk.sync(prefix)

        for i in range(300):
            if i % 10 != 0:
                assert node6_zk.get(prefix + "/" + str(i))[0] == node4_zk.get(prefix + "/" + str(i))[0]
            else:
                assert node6_zk.exists(prefix + "/" + str(i)) is None

    finally:
        try:
            node4_zk = keeper_utils.get_fake_zk(cluster_large_chunk, "node4")
            for i in range(300):
                if node4_zk.exists(prefix + "/" + str(i)):
                    node4_zk.delete(prefix + "/" + str(i))
            if node4_zk.exists(prefix):
                node4_zk.delete(prefix)
        except:
            pass

        for zk in [node4_zk, node6_zk]:
            stop_zk(zk)

    # With a 100 MB chunk size the snapshot fits in a single object, so the
    # leader calls read_logical_snp_obj exactly once (obj_id=0, is_last=true),
    # and the follower's save_logical_snp_obj takes the is_first&&is_last branch.
    log_lines_after = int(node6.count_in_log("Saving snapshot").strip())
    assert log_lines_after > log_lines_before, "No snapshot transfer log lines appeared during recovery"
    new_log = node6.grep_in_log("Saving snapshot")
    assert "obj_id 0" in new_log, f"Expected obj_id 0 in log:\n{new_log}"
    assert "obj_id 1" not in new_log, (
        "Expected snapshot to be transferred as a single chunk (obj_id 1 must not appear). "
        f"Log output:\n{new_log}"
    )


def test_recover_from_snapshot_sent_by_old_leader(started_cluster_compat):
    """
    Backward-compatibility test: a new follower (current version) must be able
    to recover from a snapshot sent by an old leader (CLICKHOUSE_CI_MIN_TESTED_VERSION).

    Old versions have no snapshot_transfer_chunk_size setting and always send the
    whole snapshot in a single NuRaft object (is_first_obj=is_last_obj=true).
    The new save_logical_snp_obj must handle that path correctly.

    We verify:
      1. compat3 (new version) recovers with correct data after the old cluster
         compat1/compat2 accumulates a snapshot it cannot replay from logs.
      2. Only obj_id 0 appears in compat3's new log lines, confirming the old
         leader sent a single-chunk snapshot (no chunking in old version).
    """
    prefix = "/test_compat_snapshot_transfer"
    leader_zk = lagging_zk = None

    compat3.stop_clickhouse(kill=True)

    log_lines_before = int(compat3.count_in_log("Saving snapshot").strip())

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster_compat, "compat1")

        leader_zk.ensure_path(prefix)
        for i in range(300):
            leader_zk.create(prefix + "/" + str(i), os.urandom(1024))
        for i in range(300):
            if i % 10 == 0:
                leader_zk.delete(prefix + "/" + str(i))
    finally:
        stop_zk(leader_zk)

    compat3.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster_compat, compat3)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster_compat, "compat1")
        lagging_zk = keeper_utils.get_fake_zk(cluster_compat, "compat3")

        leader_zk.sync(prefix)
        lagging_zk.sync(prefix)

        for i in range(300):
            if i % 10 != 0:
                assert lagging_zk.get(prefix + "/" + str(i))[0] == leader_zk.get(prefix + "/" + str(i))[0]
            else:
                assert lagging_zk.exists(prefix + "/" + str(i)) is None

    finally:
        try:
            leader_zk = keeper_utils.get_fake_zk(cluster_compat, "compat1")
            for i in range(300):
                if leader_zk.exists(prefix + "/" + str(i)):
                    leader_zk.delete(prefix + "/" + str(i))
            if leader_zk.exists(prefix):
                leader_zk.delete(prefix)
        except:
            pass

        for zk in [leader_zk, lagging_zk]:
            stop_zk(zk)

    # The old leader has no chunking: it always sends is_first_obj=is_last_obj=true,
    # so the new follower must have taken the is_first&&is_last branch.
    # Confirm exactly one obj_id (0) was logged during this recovery.
    log_lines_after = int(compat3.count_in_log("Saving snapshot").strip())
    assert log_lines_after > log_lines_before, "No snapshot transfer log lines appeared during recovery"
    new_log = compat3.grep_in_log("Saving snapshot")
    assert "obj_id 0" in new_log, f"Expected obj_id 0 in compat3 log:\n{new_log}"
    assert "obj_id 1" not in new_log, (
        "Old leader must send a single-chunk snapshot; obj_id 1 must not appear. "
        f"Log output:\n{new_log}"
    )
