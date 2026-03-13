#!/usr/bin/env python3
"""
Test for TLS certificate hot-reload on Keeper Raft connections.

This test verifies that when TLS certificates are replaced on disk and
config is reloaded, new Raft connections between Keeper nodes use the
updated certificates. A node restart is used to trigger new connections,
which then validate against the reloaded certificate material.
"""

import os
import time
import uuid

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)

# Common config files for all nodes
COMMON_CONFIGS = [
    "configs/ssl_conf.yml",
    "configs/first.crt",
    "configs/first.key",
    "configs/second.crt",
    "configs/second.key",
    "configs/rootCA.pem",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_secure_keeper1.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_secure_keeper2.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_secure_keeper3.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)

all_nodes = [node1, node2, node3]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    return ku.get_fake_zk(cluster, nodename, timeout=timeout)


def wait_nodes_ready(nodes):
    """Wait for specified nodes to be connected and form a quorum."""
    for node in nodes:
        ku.wait_until_connected(cluster, node)


def verify_cluster_works(test_path, nodes_to_check):
    """Verify the cluster can perform basic operations."""
    node_zks = []
    try:
        for node in nodes_to_check:
            node_zks.append(get_fake_zk(node.name))

        # Create a node from first node
        node_zks[0].create(test_path, b"test_data")

        # Verify from all nodes
        for i, node_zk in enumerate(node_zks):
            node_zk.sync(test_path)
            assert node_zk.exists(test_path) is not None, f"Node {i+1} cannot see {test_path}"
            data, _ = node_zk.get(test_path)
            assert data == b"test_data", f"Node {i+1} has wrong data"

        return True
    finally:
        for zk_conn in node_zks:
            if zk_conn:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except Exception:
                    pass


def replace_certificates(node):
    """
    Replace certificate files in-place.
    Copies second.crt/key over first.crt/key (the configured paths).
    """
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp /etc/clickhouse-server/config.d/second.crt /etc/clickhouse-server/config.d/first.crt && "
            "cp /etc/clickhouse-server/config.d/second.key /etc/clickhouse-server/config.d/first.key && "
            "touch /etc/clickhouse-server/config.d/first.crt /etc/clickhouse-server/config.d/first.key",
        ]
    )


def get_cert_serial_from_file(node):
    """Get the serial number of the certificate file on disk."""
    result = node.exec_in_container(
        [
            "openssl",
            "x509",
            "-in",
            "/etc/clickhouse-server/config.d/first.crt",
            "-serial",
            "-noout",
        ]
    )
    return result.strip()


def get_cert_serial_from_raft_port(node, target_host):
    """
    Connect to the Raft SSL port and get the certificate serial being served.
    This verifies the actual certificate loaded in the SSL context.
    """
    result = node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo | openssl s_client -connect {target_host}:9234 2>/dev/null | "
            "openssl x509 -serial -noout 2>/dev/null || echo 'CONNECT_FAILED'",
        ]
    )
    return result.strip()


def test_cert_reload_on_reconnect(started_cluster):
    """
    Test that restarted node uses updated certificates for new Raft connections.

    Steps:
    1. Start 3-node cluster with 'first' certificate
    2. Verify cluster works
    3. Replace certificates with 'second' on all nodes
    4. Trigger config reload
    5. Restart node3 - creates NEW Raft connections
    6. Verify cluster works (proves new certs work)
    """
    # Wait for cluster to be ready
    wait_nodes_ready(all_nodes)

    # Get initial certificate serial from file
    initial_serial = get_cert_serial_from_file(node1)
    print(f"Initial certificate serial (from file): {initial_serial}")

    # Verify the Raft port is serving the initial certificate
    initial_served = get_cert_serial_from_raft_port(node2, "node1")
    print(f"Initial certificate serial (from Raft port): {initial_served}")
    assert initial_serial == initial_served, "Raft port should serve initial cert"

    # Verify initial cluster works
    test_id = uuid.uuid4().hex[:8]
    verify_cluster_works(f"/test_initial_{test_id}", all_nodes)
    print("Initial cluster working with first certificates")

    # Replace certificate files on ALL nodes
    for node in all_nodes:
        replace_certificates(node)
    print("Replaced certificate files on all nodes")

    # Trigger config reload on all nodes
    for node in all_nodes:
        node.query("SYSTEM RELOAD CONFIG")
    print("Config reload triggered")

    # Verify the certificate file changed
    new_serial = get_cert_serial_from_file(node1)
    print(f"New certificate serial (from file): {new_serial}")
    assert initial_serial != new_serial, "Certificate serial should have changed"

    # Wait for certificate reload to complete by polling the Raft port
    # New connections should use the updated certificate
    def wait_for_cert_reload(node, target_host, expected_serial, timeout=30):
        start = time.time()
        while time.time() - start < timeout:
            served = get_cert_serial_from_raft_port(node, target_host)
            if served == expected_serial:
                return True
            time.sleep(0.5)
        return False

    # Note: Existing connections keep their old cert - that's expected.
    # The new cert is only used for NEW connections.

    # Restart node3 - this creates NEW Raft connections using new certs
    print("Restarting node3 to create new Raft connections...")
    node3.restart_clickhouse()

    # Wait for node3 to rejoin
    wait_nodes_ready([node3])
    print("Node3 restarted and reconnected")

    # Verify cluster works - proves new connections use updated certs
    verify_cluster_works(f"/test_after_restart_{test_id}", all_nodes)
    print("Cluster working after restart - new Raft connections use updated certs!")

    # Now verify the Raft port is serving the NEW certificate
    # Node3 just restarted, so its connections to node1 are fresh
    # Use polling to handle async certificate reload
    assert wait_for_cert_reload(node3, "node1", new_serial, timeout=30), (
        f"Raft port should serve new cert after reload. Expected {new_serial}"
    )
    print("Verified: Raft SSL port is serving the updated certificate!")
