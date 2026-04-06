"""
Test: UDF registry must not lose functions when Keeper connections are unstable.

Uses probabilistic iptables DROP rules to cause random Keeper failures during
refreshObjects(). With 1000 UDFs and high packet loss, some tryLoadObject() calls
will fail with hardware errors mid-loop. The fix re-throws these errors so
setAllObjects() is never called with a partial set.

We verify the fix by checking server logs for:
- "Keeper hardware error while loading user defined SQL object" — errors are detected
- "Will try to restart watching thread after error" — errors propagate and trigger retry
- "All user-defined Function objects refreshed" — eventually a full refresh succeeds
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

NUM_UDFS = 1000


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


def test_udf_refresh_retries_on_keeper_errors():
    """
    Create 1000 UDFs, then inject probabilistic ZK packet drops to cause
    hardware errors during refreshObjects(). Verify via logs that:
    1. Hardware errors are detected and re-thrown (not swallowed as nullptr)
    2. The watching thread retries after errors
    3. A full successful refresh eventually completes
    """
    create_udfs(node1, NUM_UDFS)

    assert_eq_with_retry(
        node1,
        "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'",
        f"{NUM_UDFS}\n",
    )

    with PartitionManager() as pm:
        # 90% packet drop on ZK connections — most tryLoadObject() calls will fail
        pm.add_rule({
            "instance": node1,
            "chain": "OUTPUT",
            "destination_port": 2181,
            "protocol": "tcp",
            "action": "DROP",
            "probability": 0.9,
        })

        # Force a refresh cycle by triggering session expiry then partial recovery.
        # With 90% drop rate, the session will expire quickly; some packets get through
        # so the node can partially reconnect and attempt refreshAllObjects(),
        # but individual tryLoadObject() calls will fail mid-loop.
        time.sleep(10)

    # Network restored. Wait for successful recovery.
    time.sleep(10)

    # Verify logs show the fix working:
    # 1. Hardware errors were detected in tryLoadObject()
    assert node1.contains_in_log(
        "Keeper hardware error while loading user defined SQL object"
    ), "Expected Keeper hardware errors during refresh with 90% packet drop"

    # 2. Errors propagated to the watching thread (not swallowed as nullptr)
    assert node1.contains_in_log(
        "Will try to restart watching thread after error"
    ), "Expected watching thread to catch and retry after hardware errors"

    # 3. A full refresh eventually succeeded after network was restored
    #    (the watching thread retried with a fresh session)
    assert node1.contains_in_log(
        "All user-defined Function objects refreshed"
    ), "Expected a successful full refresh after network recovery"

    # All UDFs must be intact
    count = int(
        node1.query(
            "SELECT count() FROM system.functions WHERE origin = 'SQLUserDefined' AND name LIKE 'test_udf_%'"
        ).strip()
    )
    assert count == NUM_UDFS, f"Expected {NUM_UDFS} UDFs, got {count}"

    drop_udfs(node1, NUM_UDFS)
