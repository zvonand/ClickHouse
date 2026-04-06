"""
Test: UDF registry should not lose functions when Keeper session expires during refresh.

Bug: UserDefinedSQLObjectsZooKeeperStorage::refreshObjects() loads UDFs one-by-one
from ZooKeeper. If the session expires mid-loop, tryLoadObject() returns nullptr for
the remaining UDFs. Then setAllObjects() replaces the entire in-memory registry with
the partial set — wiping out all UDFs that failed to load.

Reproduction uses iptables-based network partition (PartitionManager) to block ZK
connections from individual ClickHouse nodes. This causes a clean session expiry while
ZK itself stays healthy. On reconnect, getZooKeeper() detects a new session and calls
refreshAllObjects() — the exact code path where the bug manifests.
"""

import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
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
    for i in range(count):
        node.query(f"CREATE FUNCTION IF NOT EXISTS test_udf_{i} AS (x) -> x + {i}")


def drop_udfs(node, count):
    for i in range(count):
        node.query(f"DROP FUNCTION IF EXISTS test_udf_{i}")


def verify_all_udfs(node, count):
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
    return int(
        node.query(
            "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined'"
        ).strip()
    )


def test_udf_survives_session_expiry():
    """
    Block ZK connections from node1 via iptables to cause session expiry.
    On restore, node1 reconnects and refreshAllObjects() runs.
    All UDFs must survive the refresh cycle.
    """
    create_udfs(node1, NUM_UDFS)

    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'",
        f"{NUM_UDFS}\n",
    )

    missing = verify_all_udfs(node1, NUM_UDFS)
    assert missing == [], f"UDFs missing on node1 before partition: {missing}"

    with PartitionManager() as pm:
        # Block ZK connections from node1 — causes session expiry
        pm.drop_instance_zk_connections(node1)

        # Wait for session to expire (session_timeout_ms=3000)
        time.sleep(5)

        # UDFs should still work from in-memory cache while partitioned
        missing = verify_all_udfs(node1, NUM_UDFS)
        assert missing == [], f"UDFs missing on node1 while partitioned: {missing}"

    # PartitionManager restores connections on exit.
    # Node1 reconnects, detects new session, calls refreshAllObjects().
    # Wait for the refresh cycle to complete.
    time.sleep(5)

    # This is where the bug manifests: refreshObjects() may have loaded a partial
    # set and replaced the registry with it.
    missing = verify_all_udfs(node1, NUM_UDFS)
    assert (
        missing == []
    ), f"UDFs missing on node1 after session expiry (partial refresh bug): {missing}"

    assert get_udf_count(node1) == NUM_UDFS
    assert get_udf_count(node2) == NUM_UDFS

    drop_udfs(node1, NUM_UDFS)


def test_udf_survives_repeated_session_expiry():
    """
    Repeatedly partition node1 from ZK to trigger multiple session expiry +
    reconnect + refreshAllObjects() cycles. Each cycle is a chance for the
    partial-refresh bug to wipe the registry.
    """
    create_udfs(node1, NUM_UDFS)

    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'",
        f"{NUM_UDFS}\n",
    )

    for i in range(5):
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node1)
            # Wait for session expiry
            time.sleep(4)
        # Wait for reconnect + refresh
        time.sleep(3)

    missing = verify_all_udfs(node1, NUM_UDFS)
    assert (
        missing == []
    ), f"UDFs missing on node1 after 5 session expiry cycles: {missing}"

    missing = verify_all_udfs(node2, NUM_UDFS)
    assert (
        missing == []
    ), f"UDFs missing on node2 after 5 session expiry cycles: {missing}"

    drop_udfs(node1, NUM_UDFS)
