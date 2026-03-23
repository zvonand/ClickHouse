import os
import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/valid_rcvbuf.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_valid_rcvbuf_server_starts(started_cluster):
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


def test_invalid_rcvbuf_rejected_on_reload(started_cluster):
    """Reloading config with rcvbuf exceeding INT_MAX should be rejected."""
    # Copy invalid config into the running container
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/invalid_rcvbuf.xml"),
        "/etc/clickhouse-server/config.d/invalid_rcvbuf.xml",
    )

    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "rcvbuf" in error

    # Settings should remain unchanged
    result = node.query(
        "SELECT name, value FROM system.server_settings "
        "WHERE name IN ('http_connections_rcvbuf', 'http_connections_sndbuf') "
        "ORDER BY name"
    )
    assert "http_connections_rcvbuf\t262144" in result
    assert "http_connections_sndbuf\t262144" in result

    # Clean up: remove invalid config and restore
    node.exec_in_container(
        ["rm", "-f", "/etc/clickhouse-server/config.d/invalid_rcvbuf.xml"]
    )
    node.query("SYSTEM RELOAD CONFIG")
