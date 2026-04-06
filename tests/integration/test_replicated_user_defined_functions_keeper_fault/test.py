"""
Test: UDF registry should not lose functions when Keeper session expires during refresh.

Bug: UserDefinedSQLObjectsZooKeeperStorage::refreshObjects() loads UDFs one-by-one
from ZooKeeper. If the session expires mid-loop, tryLoadObject() returns nullptr for
the remaining UDFs. Then setAllObjects() replaces the entire in-memory registry with
the partial set — wiping out all UDFs that failed to load.

Reproduction:
1. Create N UDFs while Keeper is healthy
2. Verify all N work
3. Stop Keeper to force session expiry
4. Wait for session to expire (triggers refreshAllObjects on reconnect)
5. Start Keeper back up
6. The reconnect triggers getZooKeeper() -> refreshAllObjects() which may
   partially load UDFs if the session is still unstable
7. Verify all N UDFs still work — this is where the bug manifests:
   some UDFs will be UNKNOWN_FUNCTION because refreshObjects() replaced
   the registry with a partial set
"""

import time
from os import path as p

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
)

NUM_UDFS = 20


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_udfs(node, count):
    """Create N UDFs with distinct names."""
    for i in range(count):
        node.query(
            f"CREATE FUNCTION IF NOT EXISTS test_udf_{i} AS (x) -> x + {i}"
        )


def drop_udfs(node, count):
    """Drop N UDFs."""
    for i in range(count):
        node.query(f"DROP FUNCTION IF EXISTS test_udf_{i}")


def verify_all_udfs(node, count):
    """Verify all N UDFs exist and return correct results."""
    missing = []
    for i in range(count):
        try:
            result = node.query(f"SELECT test_udf_{i}(100)").strip()
            expected = str(100 + i)
            if result != expected:
                missing.append(f"test_udf_{i}: expected {expected}, got {result}")
        except QueryRuntimeException as e:
            missing.append(f"test_udf_{i}: {e}")
    return missing


def get_udf_count(node):
    """Get the number of user-defined functions currently registered."""
    return int(
        node.query(
            "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined'"
        ).strip()
    )


def test_udf_survives_keeper_restart():
    """
    Basic test: UDFs should survive a Keeper restart.
    After Keeper comes back, all UDFs must still be available.
    """
    create_udfs(node1, NUM_UDFS)

    # Verify all UDFs replicated to node2
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'",
        f"{NUM_UDFS}\n",
    )

    # Verify all work on both nodes
    missing1 = verify_all_udfs(node1, NUM_UDFS)
    assert missing1 == [], f"UDFs missing on node1 BEFORE keeper restart: {missing1}"

    missing2 = verify_all_udfs(node2, NUM_UDFS)
    assert missing2 == [], f"UDFs missing on node2 BEFORE keeper restart: {missing2}"

    # Stop all ZooKeeper nodes
    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

    # Wait for session to expire (session_timeout_ms=3000 in config)
    time.sleep(5)

    # UDFs should still work from cache even with Keeper down
    missing1 = verify_all_udfs(node1, NUM_UDFS)
    assert (
        missing1 == []
    ), f"UDFs missing on node1 WHILE keeper is down: {missing1}"

    # Start ZooKeeper back up
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"])

    # Wait for reconnection and refresh cycle
    time.sleep(5)

    # THIS IS THE BUG: after reconnect, refreshObjects() may have loaded a partial
    # set and replaced the registry. Some UDFs will now be UNKNOWN_FUNCTION.
    missing1 = verify_all_udfs(node1, NUM_UDFS)
    assert (
        missing1 == []
    ), f"UDFs missing on node1 AFTER keeper restart (BUG: partial refresh replaced registry): {missing1}"

    missing2 = verify_all_udfs(node2, NUM_UDFS)
    assert (
        missing2 == []
    ), f"UDFs missing on node2 AFTER keeper restart (BUG: partial refresh replaced registry): {missing2}"

    # Verify count matches
    count1 = get_udf_count(node1)
    count2 = get_udf_count(node2)
    assert (
        count1 == NUM_UDFS
    ), f"node1 has {count1} UDFs, expected {NUM_UDFS}"
    assert (
        count2 == NUM_UDFS
    ), f"node2 has {count2} UDFs, expected {NUM_UDFS}"

    drop_udfs(node1, NUM_UDFS)


def test_udf_survives_keeper_flap():
    """
    Stress test: rapidly stop/start Keeper to trigger the race condition
    where refreshObjects() is called during an unstable Keeper session.
    This is the exact scenario from the bronze-sy-26 production incident.
    """
    create_udfs(node1, NUM_UDFS)
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'",
        f"{NUM_UDFS}\n",
    )

    # Flap Keeper multiple times to trigger partial refresh cycles
    for flap in range(5):
        cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        # Short sleep — just enough for session to start expiring but not fully timeout
        time.sleep(2)
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"])
        # Short sleep — reconnect happens, triggering refreshAllObjects
        time.sleep(2)

    # After flapping, all UDFs must still be present
    # Wait for things to stabilize
    time.sleep(5)

    missing1 = verify_all_udfs(node1, NUM_UDFS)
    assert (
        missing1 == []
    ), f"UDFs missing on node1 after {5} keeper flaps: {missing1}"

    missing2 = verify_all_udfs(node2, NUM_UDFS)
    assert (
        missing2 == []
    ), f"UDFs missing on node2 after {5} keeper flaps: {missing2}"

    drop_udfs(node1, NUM_UDFS)


def test_udf_partial_keeper_outage():
    """
    Test: Stop only some ZooKeeper nodes. The remaining node should still
    serve requests, but with higher latency — which may cause some
    tryLoadObject() calls to timeout/fail while others succeed.
    """
    create_udfs(node1, NUM_UDFS)
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'",
        f"{NUM_UDFS}\n",
    )

    # Stop 2 of 3 ZK nodes — quorum is lost, then restore one
    cluster.stop_zookeeper_nodes(["zoo2", "zoo3"])
    time.sleep(4)  # Wait for session issues

    # Restore quorum with zoo2
    cluster.start_zookeeper_nodes(["zoo2"])
    cluster.wait_zookeeper_nodes_to_start(["zoo2"])
    time.sleep(5)

    # UDFs must all still be present
    missing1 = verify_all_udfs(node1, NUM_UDFS)
    assert (
        missing1 == []
    ), f"UDFs missing on node1 after partial keeper outage: {missing1}"

    # Restore full cluster
    cluster.start_zookeeper_nodes(["zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo3"])

    drop_udfs(node1, NUM_UDFS)
