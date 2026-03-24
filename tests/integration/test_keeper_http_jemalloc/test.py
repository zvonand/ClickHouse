#!/usr/bin/env python3

import pytest
import requests

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

JEMALLOC_HTTP_PORT = 9182


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start(connection_timeout=450.0)
        yield cluster
    finally:
        cluster.shutdown()


def get_url(node, path):
    return "http://{host}:{port}{path}".format(
        host=node.ip_address, port=JEMALLOC_HTTP_PORT, path=path
    )


def test_jemalloc_web_ui(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(get_url(leader, "/jemalloc"))
    assert response.status_code == 200
    assert "text/html" in response.headers["Content-Type"]
    assert "<!DOCTYPE html>" in response.text
    assert "JEMALLOC_CONFIG" in response.text
    assert "keeper" in response.text


def test_jemalloc_trailing_slash_redirects(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(get_url(leader, "/jemalloc/"), allow_redirects=False)
    assert response.status_code == 301
    assert response.headers["Location"] == "/jemalloc"


def test_jemalloc_stats(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(get_url(leader, "/jemalloc/stats"))
    assert response.status_code == 200
    assert "text/plain" in response.headers["Content-Type"]
    assert "jemalloc" in response.text.lower() or "allocated" in response.text.lower()


def test_jemalloc_status(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(get_url(leader, "/jemalloc/status"))
    assert response.status_code == 200
    assert "application/json" in response.headers["Content-Type"]

    status = response.json()
    assert "prof_enabled" in status
    assert "prof_active" in status
    assert "thread_active_init" in status
    assert "lg_sample" in status


def test_jemalloc_profile_bad_format(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(get_url(leader, "/jemalloc/profile?format=bad"))
    assert response.status_code == 400
    assert "Unknown format" in response.text


def test_jemalloc_unknown_api_path(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(get_url(leader, "/jemalloc/nonexistent"))
    assert response.status_code == 404
