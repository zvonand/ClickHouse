import pytest
import time

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # 25.10 stored implicit indices in ZooKeeper metadata.
        # Newer versions only store explicit indices, so upgrading
        # requires backward-compatible metadata comparison.
        cluster.add_instance(
            "node",
            with_zookeeper=True,
            image="clickhouse/clickhouse-server",
            tag="25.10",
            with_installed_binary=True,
            stay_alive=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def wait_for_active_replica(node, table, timeout=30):
    for _ in range(timeout):
        is_readonly = node.query(
            f"SELECT is_readonly FROM system.replicas WHERE table = '{table}';"
        ).strip()
        if is_readonly == "0":
            return
        time.sleep(1)
    assert False, f"Replica for {table} is still in readonly mode after {timeout}s"


def test_implicit_index_upgrade_numeric(started_cluster):
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_numeric;")
    node.query(
        """
        CREATE TABLE test_numeric (
            key UInt64,
            value1 Int32,
            value2 Float64,
            label String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_numeric', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=0;
        """
    )

    node.query(
        "INSERT INTO test_numeric SELECT number, number % 100, number / 3.14, toString(number) FROM numbers(10000);"
    )

    old_indices = node.query(
        "SELECT name FROM system.data_skipping_indices WHERE table = 'test_numeric' ORDER BY name;"
    ).strip()
    assert "auto_minmax_index_key" in old_indices
    assert "auto_minmax_index_value1" in old_indices
    assert "auto_minmax_index_value2" in old_indices
    # String column should not have an implicit index with this setting
    assert "auto_minmax_index_label" not in old_indices

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM test_numeric;").strip() == "10000"
    wait_for_active_replica(node, "test_numeric")

    node.query("INSERT INTO test_numeric VALUES (99999, 1, 1.0, 'x');")
    assert node.query("SELECT count() FROM test_numeric;").strip() == "10001"

    node.query("DROP TABLE test_numeric;")
    node.restart_with_original_version()


def test_implicit_index_upgrade_string(started_cluster):
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_string;")
    node.query(
        """
        CREATE TABLE test_string (
            key UInt64,
            label String,
            tag String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_string', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=0, add_minmax_index_for_string_columns=1;
        """
    )

    node.query(
        "INSERT INTO test_string SELECT number, toString(number), 'tag' FROM numbers(10000);"
    )

    old_indices = node.query(
        "SELECT name FROM system.data_skipping_indices WHERE table = 'test_string' ORDER BY name;"
    ).strip()
    assert "auto_minmax_index_label" in old_indices
    assert "auto_minmax_index_tag" in old_indices
    # Numeric column should not have an implicit index with this setting
    assert "auto_minmax_index_key" not in old_indices

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM test_string;").strip() == "10000"
    wait_for_active_replica(node, "test_string")

    node.query("INSERT INTO test_string VALUES (99999, 'x', 'y');")
    assert node.query("SELECT count() FROM test_string;").strip() == "10001"

    node.query("DROP TABLE test_string;")
    node.restart_with_original_version()


def test_implicit_index_upgrade_mixed(started_cluster):
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_mixed;")
    node.query(
        """
        CREATE TABLE test_mixed (
            key UInt64,
            value Int32,
            label String,
            tag String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mixed', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=1;
        """
    )

    node.query(
        "INSERT INTO test_mixed SELECT number, number % 100, toString(number), 'tag' FROM numbers(10000);"
    )

    old_indices = node.query(
        "SELECT name FROM system.data_skipping_indices WHERE table = 'test_mixed' ORDER BY name;"
    ).strip()
    assert "auto_minmax_index_key" in old_indices
    assert "auto_minmax_index_value" in old_indices
    assert "auto_minmax_index_label" in old_indices
    assert "auto_minmax_index_tag" in old_indices

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM test_mixed;").strip() == "10000"
    wait_for_active_replica(node, "test_mixed")

    node.query("INSERT INTO test_mixed VALUES (99999, 1, 'x', 'y');")
    assert node.query("SELECT count() FROM test_mixed;").strip() == "10001"

    node.query("DROP TABLE test_mixed;")
    node.restart_with_original_version()
