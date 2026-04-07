"""
Test: UDF registry must not lose functions when Keeper session expires.

Blocks ZK connections via iptables to force session expiry, then restores them.
After recovery, forces a refresh via SYSTEM RELOAD FUNCTIONS and verifies all
UDFs are intact — the registry was never overwritten with a partial set.
"""

import time

import pytest

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

NUM_UDFS = 100

UDF_COUNT_QUERY = "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'"


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


def test_udf_survives_keeper_session_expiry():
    """
    Create UDFs, force Keeper session expiry via network partition, restore,
    force a refresh, and verify all UDFs are still present.
    """
    if node1.is_built_with_sanitizer():
        pytest.skip("Timing-sensitive test, unreliable under sanitizers")

    create_udfs(node1, NUM_UDFS)
    assert_eq_with_retry(node1, UDF_COUNT_QUERY, f"{NUM_UDFS}\n")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)
        # Wait for session to expire (session_timeout_ms=3000 in config)
        time.sleep(5)

    # Network restored. Force a refresh cycle — this exercises the exact code
    # path (refreshAllObjects) where the bug used to replace the registry with
    # a partial set. If tryLoadObject still swallowed hardware errors, a refresh
    # during unstable recovery could lose UDFs.
    assert_eq_with_retry(
        node1,
        "SELECT 1",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    node1.query("SYSTEM RELOAD FUNCTIONS")

    # All UDFs must be intact
    assert_eq_with_retry(
        node1, UDF_COUNT_QUERY, f"{NUM_UDFS}\n", retry_count=30, sleep_time=1,
    )

    drop_udfs(node1, NUM_UDFS)
