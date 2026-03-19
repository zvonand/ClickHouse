# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node", main_configs=["configs/rocksdb.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _worker_suffix():
    """
    Return a suffix unique to this xdist worker so parallel workers use
    separate RocksDB paths and table names and do not conflict on the
    per-process RocksDB lock.  Empty string when not running under xdist.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    return f"_{worker}" if worker else ""


def test_read_only(start_cluster):
    s = _worker_suffix()
    path = f"/var/lib/clickhouse/user_files/test_rocksdb_read_only{s}"

    # cleanup any leftover state from a previous run on this worker
    for t in [f"test{s}", f"test_fail{s}", f"test_1{s}", f"test_2{s}"]:
        node.query(f"DROP TABLE IF EXISTS {t} SYNC")
    node.exec_in_container(["bash", "-c", f"rm -rf {path}"])

    # fail if read_only = true and directory does not exist.
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
        CREATE TABLE test{s} (key UInt64, value String) Engine=EmbeddedRocksDB(0, '{path}', 1) PRIMARY KEY(key);
        """
        )
    # create directory if read_only = false
    node.query(
        f"""
    CREATE TABLE test{s} (key UInt64, value String) Engine=EmbeddedRocksDB(0, '{path}') PRIMARY KEY(key);
    INSERT INTO test{s} (key, value) VALUES (0, 'a'), (1, 'b'), (2, 'c');
    """
    )
    # fail if create multiple non-read-only tables on the same directory
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
        CREATE TABLE test_fail{s} (key UInt64, value String) Engine=EmbeddedRocksDB(0, '{path}') PRIMARY KEY(key);
        """
        )
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
        CREATE TABLE test_fail{s} (key UInt64, value String) Engine=EmbeddedRocksDB(10, '{path}') PRIMARY KEY(key);
        """
        )
    # success if create multiple read-only tables on the same directory
    node.query(
        f"""
    CREATE TABLE test_1{s} (key UInt64, value String) Engine=EmbeddedRocksDB(0, '{path}', 1) PRIMARY KEY(key);
    DROP TABLE test_1{s};
    """
    )
    node.query(
        f"""
    CREATE TABLE test_2{s} (key UInt64, value String) Engine=EmbeddedRocksDB(10, '{path}', 1) PRIMARY KEY(key);
    DROP TABLE test_2{s};
    """
    )
    # success if create table on existing directory with no other tables on it
    node.query(
        f"""
    DROP TABLE test{s};
    CREATE TABLE test{s} (key UInt64, value String) Engine=EmbeddedRocksDB(10, '{path}', 1) PRIMARY KEY(key);
    """
    )
    result = node.query(f"""SELECT count() FROM test{s};""")
    assert result.strip() == "3"
    # fail if insert into table with read_only = true
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""INSERT INTO test{s} (key, value) VALUES (4, 'd');
        """
        )
    node.query(
        f"""
    DROP TABLE test{s};
    """
    )


def test_dirctory_missing_after_stop(start_cluster):
    s = _worker_suffix()
    path = f"/var/lib/clickhouse/user_files/test_rocksdb_read_only_missing{s}"

    # cleanup any leftover state from a previous run on this worker
    node.start_clickhouse()
    node.query(f"DROP TABLE IF EXISTS test_missing{s} SYNC")
    node.exec_in_container(["bash", "-c", f"rm -rf {path}"])

    # for read_only = false
    node.query(
        f"""
    CREATE TABLE test_missing{s} (key UInt64, value String) Engine=EmbeddedRocksDB(0, '{path}') PRIMARY KEY(key);
    """
    )
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"rm -r {path}",
        ]
    )
    node.start_clickhouse()
    result = node.query(
        f"""INSERT INTO test_missing{s} (key, value) VALUES (0, 'a');
    SELECT * FROM test_missing{s};
    """
    )
    assert result.strip() == "0\ta"
    node.query(
        f"""DROP TABLE test_missing{s};
    """
    )
    # for read_only = true
    node.query(
        f"""
    CREATE TABLE test_missing{s} (key UInt64, value String) Engine=EmbeddedRocksDB(0, '{path}', 1) PRIMARY KEY(key);
    """
    )
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"rm -r {path}",
        ]
    )
    node.start_clickhouse()
    with pytest.raises(QueryRuntimeException):
        node.query(f"""INSERT INTO test_missing{s} (key, value) VALUES (1, 'b');""")
    result = node.query(f"""SELECT * FROM test_missing{s};""")
    assert result.strip() == ""
    node.query(
        f"""DROP TABLE test_missing{s};
    """
    )
