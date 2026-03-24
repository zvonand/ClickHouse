import os
import pytest
import random

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_rcvbuf_server_reloads(started_cluster):
    result = node.query(
        "SELECT name, value FROM system.server_settings "
        "WHERE name IN ('http_connections_rcvbuf', 'http_connections_sndbuf') "
        "ORDER BY name"
    )
    assert "http_connections_rcvbuf\t0" in result
    assert "http_connections_sndbuf\t0" in result

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/valid_rcvbuf.xml"),
        "/etc/clickhouse-server/config.d/valid_rcvbuf.xml",
    )

    node.query("SYSTEM RELOAD CONFIG")

    """Server should start successfully with valid socket buffer settings and handle HTTP requests."""
    result = node.query(
        "SELECT name, value FROM system.server_settings "
        "WHERE name IN ('http_connections_rcvbuf', 'http_connections_sndbuf') "
        "ORDER BY name"
    )
    assert "http_connections_rcvbuf\t262144" in result
    assert "http_connections_sndbuf\t262144" in result

    # Verify outgoing HTTP connections work with the buffer settings.
    # url() table function makes an outgoing HTTP request through the connection pool.
    result = node.query(
        "SELECT count() FROM url('http://localhost:8123/?query=SELECT%201', TSV)"
    )
    assert result.strip() == "1"

    """Reloading config with rcvbuf exceeding INT_MAX should be rejected."""

    # Clean up: remove invalid config and restore
    node.exec_in_container(
        ["rm", "-f", "/etc/clickhouse-server/config.d/valid_rcvbuf.xml"]
    )

    # Copy invalid config into the running container
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/invalid_rcvbuf.xml"),
        "/etc/clickhouse-server/config.d/invalid_rcvbuf.xml",
    )

    query_id = f"test_invalid_rcvbuf_rejected_on_reload_{random.randint(10000, 99999)}"
    node.query("SYSTEM RELOAD CONFIG", query_id=query_id)
    node.query(f"""
        SYSTEM FLUSH LOGS text_log;
        SELECT throwIf( count() = 0 ) FROM system.text_log
        WHERE query_id = '{query_id}' AND message LIKE '%ignore buffer settings for HTTP%'
    """)

    # Verify outgoing HTTP connections work after rejecting invalid buffer settings.
    result = node.query(
        "SELECT count() FROM url('http://localhost:8123/?query=SELECT%201', TSV)"
    )
    assert result.strip() == "1"
