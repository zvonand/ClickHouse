"""Regression test for paginated Iceberg REST catalog listing.

Issue: https://github.com/ClickHouse/ClickHouse/issues/103674
Fix:   https://github.com/ClickHouse/ClickHouse/pull/104531

Before the fix, ``RestCatalog::getTables`` and ``RestCatalog::getNamespaces``
issued a single HTTP request and returned only the first page when the
catalog server paginates (Microsoft Fabric / OneLake / BigLake / generic
``iceberg-rest`` with page-size limits). Tables and namespaces beyond the
first page were silently invisible to ``SHOW TABLES FROM <db>`` and
``system.tables``.

The fix wraps both endpoints in a ``next-page-token`` loop. This test pins
the new behaviour by running ClickHouse against a tiny mock that *always*
paginates:

* a Python-bottle mock implements the minimum Iceberg REST surface
  (``/v1/config``, ``/v1/namespaces``, ``/v1/namespaces/{ns}/tables``);
* page sizes are deliberately set so both listings span multiple pages
  (2 namespaces with page size 1, plus 7 + 4 tables with page size 3);
* the test then asserts that ``SHOW TABLES FROM <db>`` returns ALL tables
  across ALL pages.

Without the fix, only the first page of namespaces is consulted, and only
the first page of tables is read for that single namespace -- so the result
would be 3 tables (``ns_alpha`` page 1) instead of 11.
"""

import os

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers


SCRIPT_DIR = os.path.dirname(__file__)
MOCK_SCRIPT = "iceberg_rest_paginating_mock.py"
MOCK_HOST = "iceberg_rest_mock"
MOCK_PORT = 8181

# Inventory must match ``iceberg_rest_paginating_mock.py``.
NAMESPACES = ["ns_alpha", "ns_beta"]
TABLES_PER_NAMESPACE = {
    "ns_alpha": ["t1", "t2", "t3", "t4", "t5", "t6", "t7"],
    "ns_beta":  ["s1", "s2", "s3", "s4"],
}
EXPECTED_TABLE_COUNT = sum(len(v) for v in TABLES_PER_NAMESPACE.values())  # 11

# Page sizes deliberately smaller than the lists to force pagination.
NAMESPACES_PAGE_SIZE = 1   # 2 namespaces -> 2 pages
TABLES_PAGE_SIZE = 3       # ns_alpha -> 3 pages, ns_beta -> 2 pages


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__, name="test_e2e_catalogs_pagination")
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/merge_tree.xml"],
            user_configs=["configs/allow_experimental.xml"],
            stay_alive=True,
        )
        # Companion python-bottle container that runs the paginating mock.
        # ``stop_clickhouse(kill=True)`` disables the entry-point ClickHouse
        # process so the container only runs the Python interpreter for us.
        mock = cluster.add_instance(
            name=MOCK_HOST,
            hostname=MOCK_HOST,
            image="clickhouse/python-bottle",
            tag="latest",
            stay_alive=True,
        )
        mock.stop_clickhouse(kill=True)

        cluster.start()

        start_mock_servers(
            cluster,
            SCRIPT_DIR,
            [
                (
                    MOCK_SCRIPT,
                    MOCK_HOST,
                    str(MOCK_PORT),
                    [str(NAMESPACES_PAGE_SIZE), str(TABLES_PAGE_SIZE)],
                )
            ],
        )

        yield cluster
    finally:
        cluster.shutdown()


def _create_iceberg_rest_database(node, db_name):
    base_url = f"http://{MOCK_HOST}:{MOCK_PORT}/v1"
    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(
        f"""CREATE DATABASE {db_name}
                ENGINE = DataLakeCatalog('{base_url}')
                SETTINGS catalog_type = 'rest', warehouse = 'mock'""",
        settings={"allow_database_iceberg": 1},
    )


def test_show_tables_collects_all_pages(started_cluster):
    """``SHOW TABLES`` returns tables from every page of every namespace.

    With the pre-fix code this would assert only 3 rows (``ns_alpha``
    page 1) instead of 11.
    """
    node = started_cluster.instances["node1"]
    db = "iceberg_rest_pagination_db"
    _create_iceberg_rest_database(node, db)

    rows = node.query(f"SHOW TABLES FROM {db} FORMAT TSV").strip().splitlines()

    expected = sorted(
        f"{ns}.{tbl}"
        for ns, tbls in TABLES_PER_NAMESPACE.items()
        for tbl in tbls
    )
    actual = sorted(rows)

    assert actual == expected, (
        f"SHOW TABLES returned {len(actual)} table(s); "
        f"expected {EXPECTED_TABLE_COUNT}.\n"
        f"Actual:   {actual}\nExpected: {expected}"
    )


def test_system_tables_lists_all_paginated_tables(started_cluster):
    """``system.tables`` lightweight path also collects every page."""
    node = started_cluster.instances["node1"]
    db = "iceberg_rest_pagination_systables"
    _create_iceberg_rest_database(node, db)

    # ``system.tables`` hides data lake catalog tables by default; the
    # ``SHOW TABLES`` interpreter normally toggles
    # ``show_data_lake_catalogs_in_system_tables`` for us, so set it explicitly
    # here (inline SETTINGS, applied at query level) to mirror that behaviour.
    rows = node.query(
        f"SELECT name FROM system.tables "
        f"WHERE database = '{db}' "
        f"ORDER BY name "
        f"SETTINGS show_data_lake_catalogs_in_system_tables = 1 "
        f"FORMAT TSV"
    ).strip().splitlines()
    assert len(rows) == EXPECTED_TABLE_COUNT, (
        f"system.tables returned {len(rows)} row(s); expected {EXPECTED_TABLE_COUNT}.\n"
        f"Rows: {rows}"
    )


def test_mock_actually_paginates(started_cluster):
    """Sanity check the mock itself.

    Decoupled from ClickHouse: hits the mock directly to confirm that the
    test premise holds — the listings really are split across multiple
    pages, so a non-paginating client would observe a partial result.
    """
    mock_ip = started_cluster.get_instance_ip(MOCK_HOST)
    base = f"http://{mock_ip}:{MOCK_PORT}/v1"

    first = requests.get(f"{base}/namespaces").json()
    assert "next-page-token" in first, (
        "Mock should paginate the namespaces listing on the first call"
    )
    assert len(first["namespaces"]) == NAMESPACES_PAGE_SIZE

    first_tables = requests.get(f"{base}/namespaces/ns_alpha/tables").json()
    assert "next-page-token" in first_tables, (
        "Mock should paginate ns_alpha tables on the first call"
    )
    assert len(first_tables["identifiers"]) == TABLES_PAGE_SIZE
