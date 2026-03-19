#!/usr/bin/env python3
import concurrent.futures
import math
import os
import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.keeper_snapshot_utils import (
    generate_keeper_configs,
    fill_test_tree,
    cleanup_test_tree,
    verify_test_tree,
    get_kill_timestamp,
    get_received_snapshot_info,
    assert_receiving_snapshot_logged,
    assert_obj_ids,
)


configs_dir = os.path.join(os.path.dirname(__file__), "configs")
generate_keeper_configs(configs_dir, [
    (["enable_keeper1.xml",             "enable_keeper2.xml",             "enable_keeper3.xml"],
     ["node1",      "node2",      "node3"],      4096,      False),
    (["enable_keeper4_large_chunk.xml",  "enable_keeper5_large_chunk.xml",  "enable_keeper6_large_chunk.xml"],
     ["node4",      "node5",      "node6"],      104857600, False),
    (["enable_keeper7_s3.xml",           "enable_keeper8_s3.xml",           "enable_keeper9_s3.xml"],
     ["node7",      "node8",      "node9"],      4096,      True),
    (["enable_keeper10_large_chunk_s3.xml", "enable_keeper11_large_chunk_s3.xml", "enable_keeper12_large_chunk_s3.xml"],
     ["node10",     "node11",     "node12"],     104857600, True),
    (["enable_keeper_compat1.xml",       "enable_keeper_compat2.xml",       "enable_keeper_compat3.xml"],
     ["compat1",    "compat2",    "compat3"],    None,      False),
    (["enable_keeper_compat_s3_1.xml",   "enable_keeper_compat_s3_2.xml",   "enable_keeper_compat_s3_3.xml"],
     ["compat_s3_1","compat_s3_2","compat_s3_3"],None,     True),
])

cluster = ClickHouseCluster(__file__)

# small chunk (4096 B): local and S3
node1 = cluster.add_instance("node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False)
node2 = cluster.add_instance("node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False)
node3 = cluster.add_instance("node3", main_configs=["configs/enable_keeper3.xml", "configs/text_log.xml"], stay_alive=True, with_remote_database_disk=False)

node7 = cluster.add_instance("node7", main_configs=["configs/enable_keeper7_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node8 = cluster.add_instance("node8", main_configs=["configs/enable_keeper8_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node9 = cluster.add_instance("node9", main_configs=["configs/enable_keeper9_s3.xml", "configs/text_log.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)

# large chunk (100 MB): local and S3
node4 = cluster.add_instance("node4", main_configs=["configs/enable_keeper4_large_chunk.xml"], stay_alive=True, with_remote_database_disk=False)
node5 = cluster.add_instance("node5", main_configs=["configs/enable_keeper5_large_chunk.xml"], stay_alive=True, with_remote_database_disk=False)
node6 = cluster.add_instance("node6", main_configs=["configs/enable_keeper6_large_chunk.xml", "configs/text_log.xml"], stay_alive=True, with_remote_database_disk=False)

node10 = cluster.add_instance("node10", main_configs=["configs/enable_keeper10_large_chunk_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node11 = cluster.add_instance("node11", main_configs=["configs/enable_keeper11_large_chunk_s3.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node12 = cluster.add_instance("node12", main_configs=["configs/enable_keeper12_large_chunk_s3.xml", "configs/text_log.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)

# compat: old-version leader (no chunking), new-version follower — local and S3
compat1 = cluster.add_instance("compat1", main_configs=["configs/enable_keeper_compat1.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat2 = cluster.add_instance("compat2", main_configs=["configs/enable_keeper_compat2.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat3 = cluster.add_instance("compat3", main_configs=["configs/enable_keeper_compat3.xml", "configs/text_log.xml"], stay_alive=True, with_remote_database_disk=False)

compat_s3_1 = cluster.add_instance("compat_s3_1", main_configs=["configs/enable_keeper_compat_s3_1.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat_s3_2 = cluster.add_instance("compat_s3_2", main_configs=["configs/enable_keeper_compat_s3_2.xml"], stay_alive=True, image="clickhouse/clickhouse-server", tag=CLICKHOUSE_CI_MIN_TESTED_VERSION, with_installed_binary=True, with_remote_database_disk=False)
compat_s3_3 = cluster.add_instance("compat_s3_3", main_configs=["configs/enable_keeper_compat_s3_3.xml", "configs/text_log.xml"], stay_alive=True, with_minio=True, with_remote_database_disk=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        cluster.minio_client.make_bucket("snapshots")
        yield cluster
    finally:
        cluster.shutdown()


CHUNK_SIZE = 4096  # matches snapshot_transfer_chunk_size in small-chunk configs

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


@pytest.mark.parametrize("nodes", CHUNKED_TRANSFER_PARAMS)
def test_recover_from_snapshot_with_chunked_transfer(started_cluster, nodes):
    node_leader = nodes["leader"]
    node_middle = nodes["middle"]
    node_lagging = nodes["lagging"]
    prefix = "/test_chunked_snapshot_transfer"

    cleanup_test_tree(cluster, node_leader, prefix)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    middle_zk = keeper_utils.get_fake_zk(cluster, node_middle.name)
    lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)

    leader_zk.create(prefix, b"somedata")
    middle_zk.sync(prefix)
    lagging_zk.sync(prefix)
    assert leader_zk.get(prefix)[0] == b"somedata"
    assert middle_zk.get(prefix)[0] == b"somedata"
    assert lagging_zk.get(prefix)[0] == b"somedata"

    kill_time = get_kill_timestamp(node_lagging)
    node_lagging.stop_clickhouse(kill=True)
    fill_test_tree(leader_zk, prefix)

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)
    received = get_received_snapshot_info(node_lagging, kill_time)
    assert received is not None

    assert lagging_zk.get(prefix)[0] == b"somedata"
    verify_test_tree(leader_zk, lagging_zk, prefix)
    verify_test_tree(leader_zk, middle_zk, prefix)

    snapshot_log_idx, n_chunks, snapshot_size = received
    expected_chunks = math.ceil(snapshot_size / CHUNK_SIZE)
    assert n_chunks == expected_chunks, \
        f"Expected {expected_chunks} chunks for {snapshot_size}-byte snapshot (chunk_size={CHUNK_SIZE}), got {n_chunks}"
    assert_obj_ids(node_lagging, snapshot_log_idx, list(range(n_chunks)), kill_time)

    assert_receiving_snapshot_logged(node_lagging, kill_time)


@pytest.mark.parametrize("nodes", CHUNKED_TRANSFER_PARAMS)
def test_recover_after_interrupted_transfer(started_cluster, nodes):
    """A `tmp_snapshot_X.bin` left by an interrupted transfer must not block recovery."""
    node_leader = nodes["leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_interrupted_chunked_transfer"

    cleanup_test_tree(cluster, node_leader, prefix)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    node_lagging.stop_clickhouse(kill=True)
    fill_test_tree(leader_zk, prefix)

    # Block Raft port (9234) on node_lagging before starting it.  Raft starts
    # before the TCP port (9000) is ready, so the snapshot transfer can complete
    # entirely before start_clickhouse returns.  Blocking the port prevents any
    # Raft connection until we have enabled the failpoint.
    def _drop_rule():
        node_lagging.exec_in_container(
            ["iptables", "--wait", "-D", "INPUT", "-p", "tcp", "--dport", "9234", "-j", "DROP"],
            user="root",
        )

    node_lagging.exec_in_container(
        ["iptables", "--wait", "-A", "INPUT", "-p", "tcp", "--dport", "9234", "-j", "DROP"],
        user="root",
    )
    try:
        node_lagging.start_clickhouse(20)
        node_lagging.query("SYSTEM ENABLE FAILPOINT keeper_save_snapshot_pause_mid_transfer")
    except Exception:
        _drop_rule()
        raise

    # Allow Raft to connect now that the failpoint is armed.
    _drop_rule()

    # SYSTEM WAIT FAILPOINT ... PAUSE blocks until a thread pauses at the failpoint.
    # We run it in a background thread so that the main thread can kill the node.
    # The executor must stay alive until after the assertion; otherwise leaving the
    # `with` block would call pool.shutdown(wait=True), blocking indefinitely if the
    # failpoint is never hit (the worker is stuck in SYSTEM WAIT FAILPOINT ... PAUSE).
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    wait_future = pool.submit(
        node_lagging.query,
        "SYSTEM WAIT FAILPOINT keeper_save_snapshot_pause_mid_transfer PAUSE",
    )
    done, _ = concurrent.futures.wait([wait_future], timeout=60)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        assert False, "Failpoint keeper_save_snapshot_pause_mid_transfer not triggered within 60 s"
    pool.shutdown(wait=False)

    # Thread is paused mid-transfer: tmp_snapshot_X.bin is on disk.
    node_lagging.stop_clickhouse(kill=True)

    snapshot_dir = "/var/lib/clickhouse/coordination/snapshots"
    tmp_snapshot_path = node_lagging.exec_in_container(
        ["bash", "-c", f"find {snapshot_dir} -name 'tmp_snapshot_*.bin*' | sort | tail -1 || true"]
    ).strip()
    assert tmp_snapshot_path, "No tmp_snapshot file on disk after killing mid-transfer"

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)

    assert (
        node_lagging.exec_in_container(
            ["bash", "-c", f"test -f {tmp_snapshot_path} && echo yes || echo no"]
        ).strip()
        == "no"
    ), f"tmp file was not removed on startup: {tmp_snapshot_path}"

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)
    verify_test_tree(leader_zk, lagging_zk, prefix)
    cleanup_test_tree(cluster, node_leader, prefix)


@pytest.mark.parametrize("nodes", LARGE_CHUNK_PARAMS)
def test_recover_with_chunk_size_larger_than_snapshot(started_cluster, nodes):
    """When chunk_size > snapshot size the whole snapshot is one NuRaft object (obj_id=0)."""
    node_leader = nodes["leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_large_chunk_transfer"

    cleanup_test_tree(cluster, node_leader, prefix)

    kill_time = get_kill_timestamp(node_lagging)
    node_lagging.stop_clickhouse(kill=True)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    fill_test_tree(leader_zk, prefix)

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)
    received = get_received_snapshot_info(node_lagging, kill_time)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)
    verify_test_tree(leader_zk, lagging_zk, prefix)
    cleanup_test_tree(cluster, node_leader, prefix)

    assert received is not None
    snapshot_log_idx, n_chunks, _ = received
    assert n_chunks == 1, f"Expected 1 chunk (snapshot fits within chunk_size), got {n_chunks}"
    assert_obj_ids(node_lagging, snapshot_log_idx, [0], kill_time)
    assert_receiving_snapshot_logged(node_lagging, kill_time)


@pytest.mark.parametrize("nodes", COMPAT_PARAMS)
def test_recover_from_snapshot_sent_by_old_leader(started_cluster, nodes):
    """Old leader (no chunking support) always sends a single NuRaft object (obj_id=0)."""
    node_old_leader = nodes["old_leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_compat_snapshot_transfer"

    cleanup_test_tree(cluster, node_old_leader, prefix)

    kill_time = get_kill_timestamp(node_lagging)
    node_lagging.stop_clickhouse(kill=True)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_old_leader.name)
    fill_test_tree(leader_zk, prefix)

    node_lagging.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node_lagging)
    received = get_received_snapshot_info(node_lagging, kill_time)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_old_leader.name)
    lagging_zk = keeper_utils.get_fake_zk(cluster, node_lagging.name)
    verify_test_tree(leader_zk, lagging_zk, prefix)
    cleanup_test_tree(cluster, node_old_leader, prefix)

    assert received is not None
    snapshot_log_idx, n_chunks, _ = received
    assert n_chunks == 1, f"Old leader always sends snapshot as single object, got {n_chunks} chunks"
    assert_obj_ids(node_lagging, snapshot_log_idx, [0], kill_time)
    assert_receiving_snapshot_logged(node_lagging, kill_time)
