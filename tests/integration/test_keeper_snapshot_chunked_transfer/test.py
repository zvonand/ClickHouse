#!/usr/bin/env python3
import os
import time
import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# node1 has the highest leader priority so it will be the leader.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

# A second three-node cluster configured with a large chunk size (100 MB) so that
# even a multi-hundred-KB snapshot is sent as a single NuRaft object.  This exercises
# the chunk_size > file_size code path.
cluster2 = ClickHouseCluster(__file__)

node4 = cluster2.add_instance(
    "node4",
    main_configs=["configs/enable_keeper4_large_chunk.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node5 = cluster2.add_instance(
    "node5",
    main_configs=["configs/enable_keeper5_large_chunk.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node6 = cluster2.add_instance(
    "node6",
    main_configs=["configs/enable_keeper6_large_chunk.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def started_cluster2():
    try:
        cluster2.start()
        yield cluster2
    finally:
        cluster2.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def get_fake_zk2(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster2, nodename, timeout=timeout)


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def test_recover_from_snapshot_with_chunked_transfer(started_cluster):
    """
    node3 is stopped while node1/node2 accumulate enough writes to trigger
    several snapshots. When node3 restarts it is too stale to replay logs so
    the leader must send it a snapshot. With snapshot_transfer_chunk_size=4096
    the ~300 KB snapshot is split into multiple 4 KB chunks. We verify:
      1. Data on node3 matches the rest of the cluster after recovery.
      2. The snapshot was actually transferred in more than one chunk
         (checked via log line "Saving snapshot <idx> obj_id 2" on node3).
    """
    node1_zk = node2_zk = node3_zk = None
    prefix = "/test_chunked_snapshot_transfer"

    try:
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.create(prefix, b"somedata")

        node2_zk.sync(prefix)
        node3_zk.sync(prefix)

        assert node1_zk.get(prefix)[0] == b"somedata"
        assert node2_zk.get(prefix)[0] == b"somedata"
        assert node3_zk.get(prefix)[0] == b"somedata"

        # Isolate node3 so it falls behind
        node3.stop_clickhouse(kill=True)

        # Write enough data to exceed stale_log_gap=10 and create multiple
        # snapshots (snapshot_distance=50). Use unique random bytes per node
        # so ZSTD compression doesn't shrink the snapshot below the chunk size.
        for i in range(300):
            node1_zk.create(
                prefix + str(i), os.urandom(1024)
            )

        for i in range(300):
            if i % 10 == 0:
                node1_zk.delete(prefix + str(i))

    finally:
        for zk in [node1_zk, node2_zk, node3_zk]:
            stop_zk(zk)

    # node3 is stale: it must recover via snapshot transfer (not log replay).
    node3.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node3)

    try:
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.sync(prefix)
        node2_zk.sync(prefix)
        node3_zk.sync(prefix)

        assert node1_zk.get(prefix)[0] == b"somedata"
        assert node2_zk.get(prefix)[0] == b"somedata"
        assert node3_zk.get(prefix)[0] == b"somedata"

        for i in range(300):
            if i % 10 != 0:
                value_on_1 = node1_zk.get(prefix + str(i))[0]
                assert node2_zk.get(prefix + str(i))[0] == value_on_1
                assert node3_zk.get(prefix + str(i))[0] == value_on_1
            else:
                assert node1_zk.exists(prefix + str(i)) is None
                assert node2_zk.exists(prefix + str(i)) is None
                assert node3_zk.exists(prefix + str(i)) is None

    finally:
        try:
            node1_zk = get_fake_zk("node1")
            for i in range(300):
                if node1_zk.exists(prefix + str(i)):
                    node1_zk.delete(prefix + str(i))
            if node1_zk.exists(prefix):
                node1_zk.delete(prefix)
        except:
            pass

        for zk in [node1_zk, node2_zk, node3_zk]:
            stop_zk(zk)

    # Verify that the snapshot was transferred in at least 3 chunks.
    # The follower logs "Saving snapshot <idx> obj_id <n>" for every received chunk.
    # obj_id 2 appearing means save_logical_snp_obj was called for obj_id 0, 1 and 2.
    log = node3.grep_in_log("Saving snapshot")
    assert "obj_id 2" in log, (
        "Expected snapshot to be transferred in at least 3 chunks (obj_id 2 not found in log). "
        f"Log output:\n{log}"
    )


def test_recover_after_interrupted_transfer(started_cluster):
    """
    Verify that a partial temp file left by a mid-transfer crash does not prevent
    the next recovery from succeeding.

    node3 is stopped while the leader accumulates enough data to trigger a snapshot.
    node3 is then started and killed as soon as it begins receiving snapshot chunks.
    The leader's save_logical_snp_obj with is_first_obj=true must overwrite any leftover
    temp file and the second full recovery must produce correct data.
    """
    prefix = "/test_interrupted_chunked_transfer"
    node1_zk = node3_zk = None

    try:
        node1_zk = get_fake_zk("node1")

        # node3 may still be running from the previous test; kill it so it falls behind.
        node3.stop_clickhouse(kill=True)

        node1_zk.ensure_path(prefix)
        for i in range(300):
            node1_zk.create(prefix + "/" + str(i), os.urandom(1024))
        for i in range(300):
            if i % 10 == 0:
                node1_zk.delete(prefix + "/" + str(i))
    finally:
        stop_zk(node1_zk)

    # Count "Saving snapshot" entries already in the log so we can detect new ones.
    # (The log file accumulates across restarts unless rotated.)
    snapshot_count_before = int(node3.count_in_log("Saving snapshot").strip())

    # First start: kill node3 as soon as it begins receiving snapshot chunks.
    node3.start_clickhouse(20)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        current_count = int(node3.count_in_log("Saving snapshot").strip())
        if current_count > snapshot_count_before:
            break
        time.sleep(0.5)
    node3.stop_clickhouse(kill=True)

    # Second start: let node3 recover fully from whatever temp state was left.
    node3.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster, node3)

    try:
        node1_zk = get_fake_zk("node1")
        node3_zk = get_fake_zk("node3")

        node1_zk.sync(prefix)
        node3_zk.sync(prefix)

        for i in range(300):
            if i % 10 != 0:
                assert node3_zk.get(prefix + "/" + str(i))[0] == node1_zk.get(prefix + "/" + str(i))[0]
            else:
                assert node3_zk.exists(prefix + "/" + str(i)) is None

    finally:
        try:
            node1_zk = get_fake_zk("node1")
            for i in range(300):
                if node1_zk.exists(prefix + "/" + str(i)):
                    node1_zk.delete(prefix + "/" + str(i))
            if node1_zk.exists(prefix):
                node1_zk.delete(prefix)
        except:
            pass

        for zk in [node1_zk, node3_zk]:
            stop_zk(zk)


def test_recover_with_chunk_size_larger_than_snapshot(started_cluster2):
    """
    Verify recovery when snapshot_transfer_chunk_size exceeds the snapshot file size.

    With chunk_size=104857600 (100 MB) the ~300 KB test snapshot is smaller than one
    chunk, so the leader sets is_first_obj=is_last_obj=true and sends a single NuRaft
    object.  This exercises the same single-object code path that was used before
    chunked transfer was introduced, and ensures no regression for that case.

    We verify:
      1. Data on node6 matches node4 after recovery.
      2. Only obj_id 0 appears in the follower log (single-chunk transfer).
    """
    prefix = "/test_large_chunk_transfer"
    node4_zk = node6_zk = None

    try:
        node4_zk = get_fake_zk2("node4")

        node6.stop_clickhouse(kill=True)

        node4_zk.ensure_path(prefix)
        for i in range(300):
            node4_zk.create(prefix + "/" + str(i), os.urandom(1024))
        for i in range(300):
            if i % 10 == 0:
                node4_zk.delete(prefix + "/" + str(i))
    finally:
        stop_zk(node4_zk)

    node6.start_clickhouse(20)
    keeper_utils.wait_until_connected(cluster2, node6)

    try:
        node4_zk = get_fake_zk2("node4")
        node6_zk = get_fake_zk2("node6")

        node4_zk.sync(prefix)
        node6_zk.sync(prefix)

        for i in range(300):
            if i % 10 != 0:
                assert node6_zk.get(prefix + "/" + str(i))[0] == node4_zk.get(prefix + "/" + str(i))[0]
            else:
                assert node6_zk.exists(prefix + "/" + str(i)) is None

    finally:
        try:
            node4_zk = get_fake_zk2("node4")
            for i in range(300):
                if node4_zk.exists(prefix + "/" + str(i)):
                    node4_zk.delete(prefix + "/" + str(i))
            if node4_zk.exists(prefix):
                node4_zk.delete(prefix)
        except:
            pass

        for zk in [node4_zk, node6_zk]:
            stop_zk(zk)

    # With a 100 MB chunk size, the snapshot must fit in a single object:
    # obj_id 0 must appear (first/only chunk) and obj_id 1 must not.
    log = node6.grep_in_log("Saving snapshot")
    assert "obj_id 0" in log, f"Expected obj_id 0 in log:\n{log}"
    assert "obj_id 1" not in log, (
        "Expected snapshot to be transferred as a single chunk (obj_id 1 must not appear). "
        f"Log output:\n{log}"
    )
