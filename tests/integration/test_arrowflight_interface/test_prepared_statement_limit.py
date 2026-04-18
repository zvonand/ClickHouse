# coding: utf-8

import pytest
import pyarrow as pa
import pyarrow.flight as flight
import random
import string

from .flight_sql_client import FlightSQLClient

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
        "configs/small_prepared_statements_limit.xml",
    ],
    user_configs=["configs/users_prepared_statements.xml"],
)


def get_client(username=None, password=None):
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username=username,
        password=password,
        metadata={'x-clickhouse-session-id': session_id},
        features={'metadata-reflection': 'true'},
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def test_prepared_statement_limit_exceeded():
    """Creating more prepared statements than the per-user limit should fail."""
    client = get_client()

    stmts = []
    # Limit is 3; create exactly 3 — should succeed.
    for i in range(3):
        stmt = client.prepare(f"SELECT {i + 1}")
        stmts.append(stmt)

    # The 4th should fail with a capacity error.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client.prepare("SELECT 4")

    # Clean up
    for stmt in stmts:
        stmt.close()


def test_prepared_statement_limit_after_close():
    """After closing a prepared statement, a new one can be created within the limit."""
    client = get_client()

    stmts = []
    for i in range(3):
        stmts.append(client.prepare(f"SELECT {i + 1}"))

    # Close one to free a slot.
    stmts[0].close()
    stmts.pop(0)

    # Now creating one more should succeed.
    stmts.append(client.prepare("SELECT 42"))

    # But exceeding again should fail.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client.prepare("SELECT 99")

    for stmt in stmts:
        stmt.close()


def test_prepared_statement_limit_is_per_user():
    """Different users each get their own limit."""
    client1 = get_client("user_ps1", "pass1")
    client2 = get_client("user_ps2", "pass2")

    stmts1 = []
    stmts2 = []

    # Each user can create up to 3.
    for i in range(3):
        stmts1.append(client1.prepare(f"SELECT {i + 1}"))
        stmts2.append(client2.prepare(f"SELECT {i + 10}"))

    # Both should fail on the 4th.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client1.prepare("SELECT 100")
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client2.prepare("SELECT 200")

    for stmt in stmts1 + stmts2:
        stmt.close()


def test_prepared_statement_accessible_across_sessions():
    """A prepared statement created in one session can be used from another session of the same user."""
    client1 = get_client()
    client2 = get_client()

    # Create and bind a prepared statement in the first session.
    stmt1 = client1.prepare("SELECT ? + ?")
    params = pa.record_batch(
        [pa.array([10], type=pa.int32()), pa.array([20], type=pa.int32())],
        names=["0", "1"],
    )
    stmt1.bind_parameters(params)

    # Execute the same prepared statement from the second session.
    result = client2.execute(stmt1)
    assert result.column(0).to_pylist() == [30]

    # Close from either session.
    client2.close_prepared_statement(stmt1.handle)


def test_prepared_statement_not_accessible_by_other_user():
    """A prepared statement created by one user cannot be accessed by another user."""
    client1 = get_client("user_ps1", "pass1")
    client2 = get_client("user_ps2", "pass2")

    stmt = client1.prepare("SELECT 1")

    # Another user cannot execute it.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client2.execute(stmt)

    # Another user cannot close it (silently ignored per Flight SQL spec).
    client2.close_prepared_statement(stmt.handle)
    # Verify it's still alive for the owner.
    result = client1.execute(stmt)
    assert result.column(0).to_pylist() == [1]

    # The owner can still use and close it.
    stmt.close()
