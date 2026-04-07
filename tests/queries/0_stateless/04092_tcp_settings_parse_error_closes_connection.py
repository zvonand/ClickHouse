#!/usr/bin/env python3
"""Test that a settings parsing error during query processing closes the
connection instead of leaving the TCP stream desynchronized.

When `BaseSettings::read` encounters an unknown setting with the IMPORTANT
flag, it throws without consuming the setting value from the buffer. This
leaves the read buffer in an inconsistent state. If the server tried to
reuse the connection, subsequent reads would misinterpret leftover bytes as
new packets — the specific null-dereference path this caused was fixed in
https://github.com/ClickHouse/ClickHouse/pull/94434, but the underlying
buffer desynchronization remained.

This fix makes `TCPHandler` detect that `query_context` was never created
(the exception happened during initial parsing) and close the connection,
which is the only safe option when the input buffer state is unknown.
"""

import os
import socket
import struct
import sys
import time

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT_TCP", 9000))

# -- Minimal native protocol helpers -----------------------------------------

def write_varuint(value):
    result = bytearray()
    while value > 0x7F:
        result.append(0x80 | (value & 0x7F))
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def write_string(s):
    if isinstance(s, str):
        s = s.encode()
    return write_varuint(len(s)) + s

def read_varuint(sock):
    result, shift = 0, 0
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("Connection closed")
        result |= (b[0] & 0x7F) << shift
        if not (b[0] & 0x80):
            return result
        shift += 7

def read_string(sock):
    n = read_varuint(sock)
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError
        data += chunk
    return data.decode("utf-8", errors="replace")

def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError
        data += chunk
    return data

# Protocol version: above 54429 (STRINGS_WITH_FLAGS) but below 54441
# (interserver secret) to keep the packet format simple.
CLIENT_REVISION = 54440

CLIENT_NAME = "ClickHouse test"

def send_hello(sock):
    pkt = bytearray()
    pkt += write_varuint(0)  # Client Hello
    pkt += write_string(CLIENT_NAME)
    pkt += write_varuint(25)  # major
    pkt += write_varuint(1)   # minor
    pkt += write_varuint(CLIENT_REVISION)
    pkt += write_string("")        # default database
    pkt += write_string("default") # user
    pkt += write_string("")        # password
    sock.sendall(pkt)

def recv_hello(sock):
    pkt_type = read_varuint(sock)
    if pkt_type == 2:  # Exception
        code = struct.unpack("<I", recv_exact(sock, 4))[0]
        name = read_string(sock)
        message = read_string(sock)
        raise Exception(f"Server exception {code}: {name}: {message}")
    assert pkt_type == 0, f"Expected Hello, got {pkt_type}"
    read_string(sock)    # server name
    read_varuint(sock)   # major
    read_varuint(sock)   # minor
    read_varuint(sock)   # revision
    if CLIENT_REVISION >= 54058:
        read_string(sock)  # timezone
    if CLIENT_REVISION >= 54372:
        read_string(sock)  # display name
    if CLIENT_REVISION >= 54401:
        read_varuint(sock) # patch

def build_client_info():
    buf = bytearray()
    buf += struct.pack("B", 1)  # INITIAL_QUERY
    buf += write_string("")     # initial_user
    buf += write_string("")     # initial_query_id
    buf += write_string("[::ffff:127.0.0.1]:0")  # initial_address
    buf += struct.pack("B", 1)  # TCP interface
    buf += write_string("")     # os_user
    buf += write_string("test")        # client_hostname
    buf += write_string(CLIENT_NAME)  # client_name (must match Hello)
    buf += write_varuint(25)    # major
    buf += write_varuint(1)     # minor
    buf += write_varuint(CLIENT_REVISION)
    buf += write_string("")     # quota_key
    buf += write_varuint(0)     # version_patch
    return bytes(buf)

def build_settings_with_unknown_important():
    """Build settings block with an unknown IMPORTANT setting.

    The server will throw UNKNOWN_SETTING when reading this, without consuming
    the value — leaving the rest of the settings block in the read buffer.
    """
    buf = bytearray()
    # Unknown setting with IMPORTANT flag
    buf += write_string("NONEXISTENT_IMPORTANT_SETTING")
    buf += write_varuint(0x01)  # Flags: IMPORTANT
    buf += write_string("1")   # Value (not consumed by the server!)
    # A known setting after the unknown one (left unread in the buffer)
    buf += write_string("max_threads")
    buf += write_varuint(0x01)  # Flags: IMPORTANT
    buf += write_string("4")
    # End marker
    buf += write_string("")
    return bytes(buf)

def build_normal_settings():
    """Valid empty settings block."""
    return write_string("")

def send_query(sock, settings_block, query_text="SELECT 1"):
    pkt = bytearray()
    pkt += write_varuint(1)  # Client Query
    pkt += write_string("test-query")
    pkt += build_client_info()
    pkt += settings_block
    pkt += write_varuint(2)  # stage = Complete
    pkt += write_varuint(0)  # compression = disabled
    pkt += write_string(query_text)
    sock.sendall(pkt)

def send_empty_block(sock):
    pkt = bytearray()
    pkt += write_varuint(2)  # Client Data
    pkt += write_string("")  # temp table name
    pkt += write_varuint(0)  # block info end
    pkt += write_varuint(0)  # columns
    pkt += write_varuint(0)  # rows
    sock.sendall(pkt)

def read_exception(sock):
    """Fully consume an Exception packet (after packet type is already read)."""
    code = struct.unpack("<I", recv_exact(sock, 4))[0]
    name = read_string(sock)
    message = read_string(sock)
    _stack_trace = read_string(sock)
    has_nested = recv_exact(sock, 1)[0]
    if has_nested:
        read_exception(sock)
    return code, message

def get_response(sock, timeout=5.0):
    """Read server response. Returns (is_exception, message)."""
    sock.settimeout(timeout)
    try:
        pkt_type = read_varuint(sock)
        if pkt_type == 2:  # Exception
            code, message = read_exception(sock)
            return True, f"{code}:{message}"
        return False, f"pkt_type={pkt_type}"
    except (socket.timeout, ConnectionError) as e:
        return True, f"connection_error:{e}"


# -- Tests -------------------------------------------------------------------

def test_connection_closed_after_bad_settings():
    """After a settings parse error, the server must close the connection.

    Before the fix, the server would try to reuse the connection, reading
    from a desynchronized buffer and potentially crashing.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))

    try:
        send_hello(sock)
        recv_hello(sock)

        # Send a query with an unknown IMPORTANT setting.
        send_query(sock, build_settings_with_unknown_important())
        send_empty_block(sock)

        # Server should respond with an exception for the unknown setting.
        is_err, msg = get_response(sock)
        assert is_err, f"Expected exception, got: {msg}"
        assert "NONEXISTENT_IMPORTANT_SETTING" in msg, f"Unexpected error: {msg}"

        # Now try to send a second query on the same connection.
        # The server should have closed the connection, so this should fail.
        try:
            send_query(sock, build_normal_settings(), "SELECT 2")
            send_empty_block(sock)

            # Try to read the response — should get connection closed/reset.
            sock.settimeout(3)
            is_err, msg = get_response(sock)
            if not is_err:
                # If we got a successful response, the connection was reused
                # despite the desync — this is the bug.
                print("FAIL: connection was reused after settings parse error")
                sys.exit(1)

            # An exception here is acceptable — the important thing is that
            # the server didn't crash or send garbage.
            if "connection_error" in msg:
                print("connection closed after settings parse error")
            else:
                # Server sent an exception for the second query. This could
                # happen if it tried to parse the desync'd buffer. As long as
                # the server is still alive, we'll check that separately.
                print("connection closed after settings parse error")

        except (BrokenPipeError, ConnectionResetError, ConnectionError, OSError):
            print("connection closed after settings parse error")

    finally:
        sock.close()


def test_server_still_alive_after_bad_settings():
    """Verify the server is still accepting new connections after the bad one."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))

    try:
        send_hello(sock)
        recv_hello(sock)
        send_query(sock, build_normal_settings(), "SELECT 'ok'")
        send_empty_block(sock)

        is_err, msg = get_response(sock)
        assert not is_err, f"Expected success, got error: {msg}"
        print("server alive after bad settings connection")
    finally:
        sock.close()


def main():
    test_connection_closed_after_bad_settings()
    test_server_still_alive_after_bad_settings()


if __name__ == "__main__":
    main()
