"""
Minimal Flight SQL client using pyarrow.flight and protobuf.

Replaces the `flightsql-dbapi` package (which conflicts with pyiceberg's
SQLAlchemy>=2 requirement) with just the subset the integration tests need:
  - FlightSQLClient  (execute, execute_update, do_get)
  - flight_descriptor helper
  - CommandStatementQuery, CommandStatementUpdate, DoPutUpdateResult protobufs
"""

from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
from google.protobuf import any_pb2
from pyarrow import flight

# ---------------------------------------------------------------------------
# Protobuf definitions generated from the Arrow Flight SQL .proto
# Only the messages actually used by the tests are imported here;
# the full set is available in the module-level namespace after
# BuildTopDescriptorsAndMessages runs.
# ---------------------------------------------------------------------------
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2 as _descriptor_pb2

_sym_db = _symbol_database.Default()

_DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x19\x66lightsql/flightsql.proto\x12\x19\x61rrow.flight.protocol.sql'
    b'\x1a google/protobuf/descriptor.proto'
    b'\"&\n\x11\x43ommandGetSqlInfo\x12\x0c\n\x04info\x18\x01 \x03(\r:\x03\xc0>\x01'
    b'\"[\n\x15\x43ommandStatementQuery\x12\r\n\x05query\x18\x01 \x01(\t'
    b'\x12\x1b\n\x0etransaction_id\x18\x02 \x01(\x0cH\x00\x88\x01\x01:\x03\xc0>\x01'
    b'\x42\x11\n\x0f_transaction_id'
    b'\"\\\n\x16\x43ommandStatementUpdate\x12\r\n\x05query\x18\x01 \x01(\t'
    b'\x12\x1b\n\x0etransaction_id\x18\x02 \x01(\x0cH\x00\x88\x01\x01:\x03\xc0>\x01'
    b'\x42\x11\n\x0f_transaction_id'
    b'\".\n\x11\x44oPutUpdateResult\x12\x14\n\x0crecord_count\x18\x01 \x01(\x03:\x03\xc0>\x01'
    b':6\n\x0c\x65xperimental\x12\x1f.google.protobuf.MessageOptions\x18\xe8\x07 \x01(\x08'
    b'\x42\x02H\x01'
    b'b\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(_DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(_DESCRIPTOR, 'flightsql.flightsql_pb2', _globals)

# Expose the three message classes at module level.
CommandStatementQuery = _globals['CommandStatementQuery']
CommandStatementUpdate = _globals['CommandStatementUpdate']
DoPutUpdateResult = _globals['DoPutUpdateResult']


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def flight_descriptor(command: Any) -> flight.FlightDescriptor:
    """Pack a protobuf command into a FlightDescriptor."""
    wrapper = any_pb2.Any()
    wrapper.Pack(command)
    return flight.FlightDescriptor.for_command(wrapper.SerializeToString())


def _create_flight_client(
    host: str = "localhost",
    port: int = 443,
    insecure: Optional[bool] = None,
    disable_server_verification: Optional[bool] = None,
    metadata: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Tuple[flight.FlightClient, List[Tuple[bytes, bytes]]]:
    protocol = "tls"
    if insecure:
        protocol = "tcp"
    elif disable_server_verification:
        kwargs["disable_server_verification"] = True

    url = f"grpc+{protocol}://{host}:{port}"
    client = flight.FlightClient(url, **kwargs)

    headers: List[Tuple[bytes, bytes]] = []
    for k, v in (metadata or {}).items():
        headers.append((k.encode("utf-8"), v.encode("utf-8")))

    return client, headers


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class FlightSQLClient:
    """
    Thin Flight SQL wrapper around pyarrow.flight.FlightClient.

    Implements only the subset used by the ClickHouse integration tests:
    execute, execute_update, do_get, plus access to the underlying client.
    """

    def __init__(self, *args, features: Optional[Dict[str, str]] = None, **kwargs):
        client, headers = _create_flight_client(*args, **kwargs)
        self.client = client
        self.headers = headers
        self.features = features or {}

    def _flight_call_options(self):
        headers = list(OrderedDict(self.headers).items())
        return flight.FlightCallOptions(headers=headers)

    def execute(self, query: str) -> flight.FlightInfo:
        """Execute a query and return FlightInfo for result retrieval."""
        cmd = CommandStatementQuery(query=query)
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def execute_update(self, query: str) -> int:
        """Execute a DDL/DML statement and return the affected row count."""
        cmd = CommandStatementUpdate(query=query)
        desc = flight_descriptor(cmd)
        options = self._flight_call_options()
        writer, reader = self.client.do_put(
            desc, pa.schema([]), options
        )
        result = reader.read()
        writer.close()

        if result is None:
            return 0
        update_result = DoPutUpdateResult()
        update_result.ParseFromString(result.to_pybytes())
        return update_result.record_count

    def do_get(self, ticket) -> flight.FlightStreamReader:
        """Retrieve Arrow data for a given ticket."""
        options = self._flight_call_options()
        return self.client.do_get(ticket, options)
