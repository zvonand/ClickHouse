#!/usr/bin/env python3
import math
import os
import re
import time
import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Small-chunk clusters (snapshot_transfer_chunk_size=4096): local and S3.
node1 = cluster.add_instance("node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False)
node2 = cluster.add_instance("node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False)
node3 = cluster.add_instance("node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True, with_remote_database_disk=False)

node7 = cluster.add_instance("node7", main_configs=["configs/enable_keeper7_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node8 = cluster.add_instance("node8", main_configs=["configs/enable_keeper8_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node9 = cluster.add_instance("node9", main_configs=["configs/enable_keeper9_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)

# Large-chunk clusters (snapshot_transfer_chunk_size=100MB): local and S3.
node4 = cluster.add_instance("node4", main_configs=["configs/enable_keeper4_large_chunk.xml"], stay_alive=True, with_remote_database_disk=False)
node5 = cluster.add_instance("node5", main_configs=["configs/enable_keeper5_large_chunk.xml"], stay_alive=True, with_remote_database_disk=False)
node6 = cluster.add_instance("node6", main_configs=["configs/enable_keeper6_large_chunk.xml"], stay_alive=True, with_remote_database_disk=False)

node10 = cluster.add_instance("node10", main_configs=["configs/enable_keeper10_large_chunk_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node11 = cluster.add_instance("node11", main_configs=["configs/enable_keeper11_large_chunk_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node12 = cluster.add_instance("node12", main_configs=["configs/enable_keeper12_large_chunk_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)

# Compat clusters: old-version leader, new-version follower — local and S3.
# Old versions have no snapshot_transfer_chunk_size and always send a single NuRaft object.
compat1 = cluster.add_instance("compat1", main_configs=["configs/enable_keeper_compat1.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat2 = cluster.add_instance("compat2", main_configs=["configs/enable_keeper_compat2.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat3 = cluster.add_instance("compat3", main_configs=["configs/enable_keeper_compat3.xml"], stay_alive=True, with_remote_database_disk=False)

compat_s3_1 = cluster.add_instance("compat_s3_1", main_configs=["configs/enable_keeper_compat_s3_1.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat_s3_2 = cluster.add_instance("compat_s3_2", main_configs=["configs/enable_keeper_compat_s3_2.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat_s3_3 = cluster.add_instance("compat_s3_3", main_configs=["configs/enable_keeper_compat_s3_3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        cluster.minio_client.make_bucket("snapshots")
        yield cluster
    finally:
        cluster.shutdown()


CHUNK_SIZE = 4096  # snapshot_transfer_chunk_size for small-chunk clusters

CHUNKED_TRANSFER_PARAMS = [
    pytest.param({"leader": node1, "middle": node2, "lagging": node3}, id="local_disk"),
    pytest.param({"leader": node7, "middle": node8, "lagging": node9}, id="s3_disk"),
]

LARGE_CHUNK_PARAMS = [
    pytest.param({"leader": node4, "lagging": node6}, id="local_disk"),
    pytest.param({"leader": node10, "lagging": node12}, id="s3_disk"),
]

COMPAT_PARAMS = [
    pytest.param({"old_leader": compat1, "lagging": compat3}, id="local_disk"),
    pytest.param({"old_leader": compat_s3_1, "lagging": compat_s3_3}, id="s3_disk"),
]


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def fill_test_tree(zk, base, count=300):
    """Create `count` children under `base`, then delete every 10th."""
    zk.ensure_path(base)
    for i in range(count):
        zk.create(f"{base}/{i}", os.urandom(1024))
    for i in range(0, count, 10):
        zk.delete(f"{base}/{i}")


def cleanup_test_tree(leader_node, base, count=300):
    """Delete all nodes created by `fill_test_tree`."""
    zk = None
    try:
        zk = keeper_utils.get_fake_zk(cluster, leader_node.name)
        for i in range(count):
            if zk.exists(f"{base}/{i}"):
                zk.delete(f"{base}/{i}")
        if zk.exists(base):
            zk.delete(base)
    except:
        pass
    finally:
        stop_zk(zk)


def verify_test_tree(leader_zk, lagging_zk, base, count=300):
    """Assert node_lagging has the same data as node_leader after recovery."""
    leader_zk.sync(base)
    lagging_zk.sync(base)
    for i in range(count):
        if i % 10 != 0:
            assert lagging_zk.get(f"{base}/{i}")[0] == leader_zk.get(f"{base}/{i}")[0]
        else:
            assert lagging_zk.exists(f"{base}/{i}") is None


def get_log_line_count(node):
    """Return the current number of lines in the clickhouse-server log."""
    result = node.exec_in_container(
        ["bash", "-c",
         "wc -l < /var/log/clickhouse-server/clickhouse-server.log || echo 0"]
    ).strip()
    return int(result)


def get_snapshot_log_lines_for_idx(node, snapshot_log_idx, from_line=0):
    """Return "Saving snapshot <snapshot_log_idx> obj_id …" lines from the node log.

    from_line -- skip the first from_line lines (so only lines written after a node restart
    are searched, avoiding false matches from previous test iterations).
    """
    output = node.exec_in_container(
        ["bash", "-c",
         f"tail -n +{from_line + 1} /var/log/clickhouse-server/clickhouse-server.log"
         f" | grep 'Saving snapshot {snapshot_log_idx} obj_id' || true"]
    )
    return [line for line in output.splitlines() if line]


def get_received_snapshot_info(node, from_line):
    """Return (log_idx, size_bytes) for the snapshot received via chunked transfer, or None.

    Searches only in log lines written after from_line, so repeated test runs don't pick
    up locally-created snapshots from earlier iterations.  Returns None if no snapshot was
    received (node caught up via log replay instead), in which case the caller should skip
    the obj_id check but still verify data correctness.
    """
    output = node.exec_in_container(
        ["bash", "-c",
         f"tail -n +{from_line + 1} /var/log/clickhouse-server/clickhouse-server.log"
         " | grep 'Saving snapshot .* obj_id' || true"]
    )
    lines = [line for line in output.splitlines() if line]
    if not lines:
        return None
    m = re.search(r"Saving snapshot (\d+) obj_id", lines[0])
    if not m:
        return None
    log_idx = int(m.group(1))
    size_result = node.exec_in_container(
        ["bash", "-c",
         f"find /var/lib/clickhouse/coordination/snapshots/ -name 'snapshot_{log_idx}.bin*'"
         r" -printf '%s\n' | head -1"]
    ).strip()
    if not size_result:
        return None
    return log_idx, int(size_result)


def assert_obj_ids(node_lagging, snapshot_log_idx, expected, from_line=0):
    """Assert that the obj_ids logged during snapshot transfer cover `expected`.

    NuRaft may send a small number of duplicate chunks when multiple heartbeat
    threads fire before the first ACK returns.  We tolerate at most
    len(expected) // 2 excess receives (i.e. at least half the chunks must
    arrive exactly once) to catch systematic duplication bugs.
    """
    lines = get_snapshot_log_lines_for_idx(node_lagging, snapshot_log_idx, from_line)
    assert lines, "No 'Saving snapshot' log lines appeared during recovery"
    all_ids = [int(m.group(1)) for line in lines if (m := re.search(r"obj_id (\d+)", line))]
    duplicates = len(all_ids) - len(set(all_ids))
    max_allowed = len(expected) // 2
    assert set(all_ids) == set(expected), f"Expected obj_ids={set(expected)}, got: {sorted(set(all_ids))}"
    assert duplicates <= max_allowed, \
        f"Too many duplicate chunks: {duplicates} (max {max_allowed}), obj_ids={all_ids}"


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("nodes", CHUNKED_TRANSFER_PARAMS)
def test_recover_from_snapshot_with_chunked_transfer(started_cluster, nodes):
    """
    Verify chunked snapshot transfer: node_lagging falls behind, then recovers via
    snapshot. With snapshot_transfer_chunk_size=4096 the snapshot is split into
    multiple 4 KB chunks. Asserts data correctness and that obj_ids form [0, N).
    """
    node_leader = nodes["leader"]
    node_middle = nodes["middle"]
    node_lagging = nodes["lagging"]
    prefix = "/test_chunked_snapshot_transfer"
    leader_zk = middle_zk = lagging_zk = None

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
        middle_zk = keeper_utils.get_fake_zk(cluster, node_middle.name)
        lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)

        leader_zk.create(prefix, b"somedata")
        middle_zk.sync(prefix)
        lagging_zk.sync(prefix)
        assert leader_zk.get(prefix)[0] == b"somedata"
        assert middle_zk.get(prefix)[0] == b"somedata"
        assert lagging_zk.get(prefix)[0] == b"somedata"

        log_offset = get_log_line_count(node_lagging)
        node_lagging.stop_clickhouse(kill=True)
        # Write enough data to exceed stale_log_gap=10 and trigger multiple snapshots.
        # Random bytes prevent ZSTD from shrinking below chunk size.
        fill_test_tree(leader_zk, prefix)
    finally:
        for zk in [leader_zk, middle_zk, lagging_zk]:
            stop_zk(zk)

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)
    # Find the snapshot received via chunked transfer (not locally created ones).
    received = get_received_snapshot_info(node_lagging, log_offset)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
        middle_zk = keeper_utils.get_fake_zk(cluster, node_middle.name)
        lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)

        assert lagging_zk.get(prefix)[0] == b"somedata"
        verify_test_tree(leader_zk, lagging_zk, prefix)
        verify_test_tree(leader_zk, middle_zk, prefix)
    finally:
        cleanup_test_tree(node_leader, prefix)
        for zk in [leader_zk, middle_zk, lagging_zk]:
            stop_zk(zk)

    if received is not None:
        snapshot_log_idx, snapshot_size = received
        expected_chunks = math.ceil(snapshot_size / CHUNK_SIZE)
        assert_obj_ids(node_lagging, snapshot_log_idx, list(range(expected_chunks)), from_line=log_offset)


@pytest.mark.parametrize("nodes", CHUNKED_TRANSFER_PARAMS)
def test_recover_after_interrupted_transfer(started_cluster, nodes):
    """
    Verify that a partial tmp file left by a mid-transfer kill does not prevent
    the next full recovery.

    Uses the keeper_save_snapshot_pause_mid_transfer fail point to pause
    node_lagging deterministically while writing a middle chunk, then kills it.
    This leaves a real tmp_snapshot_X.bin on disk.  On the next start the cleanup
    logic in KeeperSnapshotManager removes it and recovery completes correctly.
    """
    node_leader = nodes["leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_interrupted_chunked_transfer"
    leader_zk = lagging_zk = None

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
        node_lagging.stop_clickhouse(kill=True)
        fill_test_tree(leader_zk, prefix)
    finally:
        stop_zk(leader_zk)

    # First start: pause mid-transfer, then kill to leave tmp_snapshot_X.bin on disk.
    node_lagging.start_clickhouse(20)
    node_lagging.query("SYSTEM ENABLE FAILPOINT keeper_save_snapshot_pause_mid_transfer")
    log_offset = get_log_line_count(node_lagging)

    # Wait until a non-first chunk is being processed — the thread is now paused
    # at the fail point so no further chunks will be logged until we kill the node.
    deadline = time.monotonic() + 30
    mid_chunk_seen = False
    while time.monotonic() < deadline:
        output = node_lagging.exec_in_container(
            ["bash", "-c",
             f"tail -n +{log_offset + 1} /var/log/clickhouse-server/clickhouse-server.log"
             " | grep -E 'Saving snapshot [0-9]+ obj_id [1-9]' || true"]
        )
        if output.strip():
            mid_chunk_seen = True
            break
        time.sleep(0.5)

    assert mid_chunk_seen, "Fail point was not triggered: no middle chunk seen within timeout"
    node_lagging.stop_clickhouse(kill=True)  # leaves tmp_snapshot_X.bin on disk

    # Second start: cleanup removes tmp_snapshot_X.bin, then fresh full recovery.
    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
        lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)
        verify_test_tree(leader_zk, lagging_zk, prefix)
    finally:
        cleanup_test_tree(node_leader, prefix)
        for zk in [leader_zk, lagging_zk]:
            stop_zk(zk)


@pytest.mark.parametrize("nodes", LARGE_CHUNK_PARAMS)
def test_recover_with_chunk_size_larger_than_snapshot(started_cluster, nodes):
    """
    Verify recovery when snapshot_transfer_chunk_size (100 MB) exceeds snapshot size.
    The whole snapshot is sent as a single NuRaft object (is_first_obj=is_last_obj=true),
    so obj_ids must be exactly [0].
    """
    node_leader = nodes["leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_large_chunk_transfer"
    leader_zk = lagging_zk = None

    log_offset = get_log_line_count(node_lagging)
    node_lagging.stop_clickhouse(kill=True)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
        fill_test_tree(leader_zk, prefix)
    finally:
        stop_zk(leader_zk)

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)
    received = get_received_snapshot_info(node_lagging, log_offset)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
        lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)
        verify_test_tree(leader_zk, lagging_zk, prefix)
    finally:
        cleanup_test_tree(node_leader, prefix)
        for zk in [leader_zk, lagging_zk]:
            stop_zk(zk)

    if received is not None:
        snapshot_log_idx, _ = received
        assert_obj_ids(node_lagging, snapshot_log_idx, [0], from_line=log_offset)


@pytest.mark.parametrize("nodes", COMPAT_PARAMS)
def test_recover_from_snapshot_sent_by_old_leader(started_cluster, nodes):
    """
    Backward-compatibility: a new follower must recover from a snapshot sent by an
    old leader (CLICKHOUSE_CI_MIN_TESTED_VERSION) that has no chunking support and
    always sends is_first_obj=is_last_obj=true. Asserts obj_ids == [0].
    """
    node_old_leader = nodes["old_leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_compat_snapshot_transfer"
    leader_zk = lagging_zk = None

    log_offset = get_log_line_count(node_lagging)
    node_lagging.stop_clickhouse(kill=True)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_old_leader.name)
        fill_test_tree(leader_zk, prefix)
    finally:
        stop_zk(leader_zk)

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)
    received = get_received_snapshot_info(node_lagging, log_offset)

    try:
        leader_zk = keeper_utils.get_fake_zk(cluster, node_old_leader.name)
        lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)
        verify_test_tree(leader_zk, lagging_zk, prefix)
    finally:
        cleanup_test_tree(node_old_leader, prefix)
        for zk in [leader_zk, lagging_zk]:
            stop_zk(zk)

    if received is not None:
        snapshot_log_idx, _ = received
        assert_obj_ids(node_lagging, snapshot_log_idx, [0], from_line=log_offset)
