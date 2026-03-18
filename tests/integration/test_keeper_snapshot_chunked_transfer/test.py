#!/usr/bin/env python3
import concurrent.futures
import math
import os
import re
import time
import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster


def _generate_keeper_configs():
    def make_config(server_id, hosts, chunk_size=None, use_s3=False):
        s3_block = (
            "\n        <s3_snapshot>"
            "\n            <endpoint>http://minio1:9001/snapshots/</endpoint>"
            "\n            <access_key_id>minio</access_key_id>"
            "\n            <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>"
            "\n        </s3_snapshot>"
        ) if use_s3 else ""
        chunk_line = f"\n            <snapshot_transfer_chunk_size>{chunk_size}</snapshot_transfer_chunk_size>" if chunk_size else ""
        prios = [70, 20, 10]
        servers = []
        for i, (host, prio) in enumerate(zip(hosts, prios), start=1):
            follower = "\n                <start_as_follower>true</start_as_follower>" if i > 1 else ""
            servers.append(
                f"            <server>\n"
                f"                <id>{i}</id>\n"
                f"                <hostname>{host}</hostname>\n"
                f"                <port>9234</port>\n"
                f"                <can_become_leader>true</can_become_leader>{follower}\n"
                f"                <priority>{prio}</priority>\n"
                f"            </server>"
            )
        return (
            f"<clickhouse>\n"
            f"    <keeper_server>{s3_block}\n"
            f"        <tcp_port>9181</tcp_port>\n"
            f"        <server_id>{server_id}</server_id>\n"
            f"\n"
            f"        <coordination_settings>\n"
            f"            <operation_timeout_ms>5000</operation_timeout_ms>\n"
            f"            <session_timeout_ms>10000</session_timeout_ms>\n"
            f"            <raft_logs_level>trace</raft_logs_level>\n"
            f"            <snapshot_distance>50</snapshot_distance>\n"
            f"            <stale_log_gap>10</stale_log_gap>\n"
            f"            <reserved_log_items>1</reserved_log_items>{chunk_line}\n"
            f"        </coordination_settings>\n"
            f"\n"
            f"        <raft_configuration>\n"
            + "\n".join(servers) + "\n"
            f"        </raft_configuration>\n"
            f"    </keeper_server>\n"
            f"</clickhouse>\n"
        )

    configs_dir = os.path.join(os.path.dirname(__file__), "configs")
    os.makedirs(configs_dir, exist_ok=True)

    clusters = [
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
    ]

    for filenames, hosts, chunk_size, use_s3 in clusters:
        for server_id, filename in enumerate(filenames, start=1):
            path = os.path.join(configs_dir, filename)
            with open(path, "w") as f:
                f.write(make_config(server_id, hosts, chunk_size, use_s3))


_generate_keeper_configs()

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


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def fill_test_tree(zk, base, count=300):
    zk.ensure_path(base)
    for i in range(count):
        zk.create(f"{base}/{i}", os.urandom(1024))  # random to resist ZSTD compression
    for i in range(0, count, 10):
        zk.delete(f"{base}/{i}")


def cleanup_test_tree(leader_node, base):
    zk = None
    try:
        zk = keeper_utils.get_fake_zk(cluster, leader_node.name)
        if zk.exists(base):
            zk.delete(base, recursive=True)
    except:
        pass
    finally:
        stop_zk(zk)


def verify_test_tree(leader_zk, lagging_zk, base, count=300):
    leader_zk.sync(base)
    lagging_zk.sync(base)
    for i in range(count):
        if i % 10 != 0:
            assert lagging_zk.get(f"{base}/{i}")[0] == leader_zk.get(f"{base}/{i}")[0]
        else:
            assert lagging_zk.exists(f"{base}/{i}") is None


def get_kill_timestamp(node):
    return node.query("SELECT now64(6)").strip()


def _query_text_log(node, after_time, pattern, timeout=15):
    deadline = time.time() + timeout
    while True:
        try:
            node.query("SYSTEM FLUSH LOGS")
            result = node.query(
                f"SELECT message FROM system.text_log "
                f"WHERE event_time_microseconds > '{after_time}' "
                f"AND message LIKE '{pattern}' "
                f"ORDER BY event_time_microseconds"
            ).strip()
            if result:
                return [line for line in result.splitlines() if line]
        except Exception:
            pass

        if time.time() >= deadline:
            return []
        time.sleep(1)


def get_received_snapshot_info(node, after_time, timeout=15):
    lines = _query_text_log(node, after_time, "Saved snapshot % chunks, % bytes)", timeout)
    if not lines:
        return None
    m = re.search(r"Saved snapshot (\d+) \((\d+) chunks, (\d+) bytes\)", lines[-1])
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def get_snapshot_log_lines_for_idx(node, snapshot_log_idx, after_time, timeout=15):
    return _query_text_log(
        node, after_time, f"Saving snapshot {snapshot_log_idx} obj_id %", timeout
    )


def assert_receiving_snapshot_logged(node_lagging, after_time):
    """Assert that `save_logical_snp_obj` fired on the first chunk (proves the snapshot was actually received)."""
    lines = _query_text_log(node_lagging, after_time, "Receiving snapshot % to % disk", timeout=15)
    assert lines, f"Expected 'Receiving snapshot % to % disk' in system.text_log on {node_lagging.name}"


def assert_obj_ids(node_lagging, snapshot_log_idx, expected, after_time):
    lines = get_snapshot_log_lines_for_idx(node_lagging, snapshot_log_idx, after_time)
    assert lines, "No 'Saving snapshot' log lines appeared during recovery"
    all_ids = [int(m.group(1)) for line in lines if (m := re.search(r"obj_id (\d+)", line))]
    duplicates = len(all_ids) - len(set(all_ids))
    # NuRaft may re-send a chunk when a heartbeat fires before the first ACK returns;
    # tolerate at most len(expected)//2 duplicates to catch systematic bugs.
    max_allowed = len(expected) // 2
    assert set(all_ids) == set(expected), f"Expected obj_ids={set(expected)}, got: {sorted(set(all_ids))}"
    assert duplicates <= max_allowed, \
        f"Too many duplicate chunks: {duplicates} (max {max_allowed}), obj_ids={all_ids}"


@pytest.mark.parametrize("nodes", CHUNKED_TRANSFER_PARAMS)
def test_recover_from_snapshot_with_chunked_transfer(started_cluster, nodes):
    node_leader = nodes["leader"]
    node_middle = nodes["middle"]
    node_lagging = nodes["lagging"]
    prefix = "/test_chunked_snapshot_transfer"

    cleanup_test_tree(node_leader, prefix)

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

    cleanup_test_tree(node_leader, prefix)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    node_lagging.stop_clickhouse(kill=True)
    fill_test_tree(leader_zk, prefix)

    # Block Raft port (9234) on node_lagging before starting it.  Raft starts
    # before the TCP port (9000) is ready, so the snapshot transfer can complete
    # entirely before start_clickhouse returns.  Blocking the port prevents any
    # Raft connection until we have enabled the failpoint.
    node_lagging.exec_in_container(
        ["iptables", "--wait", "-A", "INPUT", "-p", "tcp", "--dport", "9234", "-j", "DROP"],
        user="root",
    )

    node_lagging.start_clickhouse(20)
    node_lagging.query("SYSTEM ENABLE FAILPOINT keeper_save_snapshot_pause_mid_transfer")

    # Allow Raft to connect now that the failpoint is armed.
    node_lagging.exec_in_container(
        ["iptables", "--wait", "-D", "INPUT", "-p", "tcp", "--dport", "9234", "-j", "DROP"],
        user="root",
    )

    # SYSTEM WAIT FAILPOINT ... PAUSE blocks until a thread pauses at the failpoint.
    # We run it in a background thread so that the main thread can kill the node.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        wait_future = pool.submit(
            node_lagging.query,
            "SYSTEM WAIT FAILPOINT keeper_save_snapshot_pause_mid_transfer PAUSE",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
    assert done, "Failpoint keeper_save_snapshot_pause_mid_transfer not triggered within 60 s"

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
    cleanup_test_tree(node_leader, prefix)


@pytest.mark.parametrize("nodes", LARGE_CHUNK_PARAMS)
def test_recover_with_chunk_size_larger_than_snapshot(started_cluster, nodes):
    """When chunk_size > snapshot size the whole snapshot is one NuRaft object (obj_id=0)."""
    node_leader = nodes["leader"]
    node_lagging = nodes["lagging"]
    prefix = "/test_large_chunk_transfer"

    cleanup_test_tree(node_leader, prefix)

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
    cleanup_test_tree(node_leader, prefix)

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

    cleanup_test_tree(node_old_leader, prefix)

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
    cleanup_test_tree(node_old_leader, prefix)

    assert received is not None
    snapshot_log_idx, n_chunks, _ = received
    assert n_chunks == 1, f"Old leader always sends snapshot as single object, got {n_chunks} chunks"
    assert_obj_ids(node_lagging, snapshot_log_idx, [0], kill_time)
    assert_receiving_snapshot_logged(node_lagging, kill_time)
