import os
import re
import time


def generate_keeper_configs(configs_dir, clusters):
    """Generate Keeper XML config files for the given cluster definitions.

    clusters is a list of (filenames, hosts, chunk_size, use_s3) tuples where:
      - filenames: output XML file names, one per server
      - hosts:     hostname for each server (same length as filenames)
      - chunk_size: snapshot_transfer_chunk_size value, or None to omit
      - use_s3:    whether to add an S3 snapshot block
    """
    def make_config(server_id, hosts, chunk_size, use_s3):
        s3_block = (
            "\n        <s3_snapshot>"
            "\n            <endpoint>http://minio1:9001/snapshots/</endpoint>"
            "\n            <access_key_id>minio</access_key_id>"
            "\n            <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>"
            "\n        </s3_snapshot>"
        ) if use_s3 else ""
        chunk_line = (
            f"\n            <snapshot_transfer_chunk_size>{chunk_size}</snapshot_transfer_chunk_size>"
            if chunk_size else ""
        )
        # Assign decreasing priorities: first node is most likely to become leader.
        base_prios = [70, 20, 10, 5, 3]
        prios = base_prios[:len(hosts)]
        servers = []
        for i, (host, prio) in enumerate(zip(hosts, prios), start=1):
            follower = "\n                <start_as_follower>true</start_as_follower>" if i > 1 else ""
            servers.append(
                f"            <server>\n"
                f"                <id>{i}</id>\n"
                f"                <hostname>{host}</hostname>\n"
                f"                <port>9234</port>\n"
                f"                <can_become_leader>true</can_become_leader>{follower}\n"
                f"                <priority>{prio}</priority>\n"
                f"            </server>"
            )
        return (
            f"<clickhouse>\n"
            f"    <keeper_server>{s3_block}\n"
            f"        <tcp_port>9181</tcp_port>\n"
            f"        <server_id>{server_id}</server_id>\n"
            f"\n"
            f"        <coordination_settings>\n"
            f"            <operation_timeout_ms>5000</operation_timeout_ms>\n"
            f"            <session_timeout_ms>10000</session_timeout_ms>\n"
            f"            <raft_logs_level>trace</raft_logs_level>\n"
            f"            <snapshot_distance>50</snapshot_distance>\n"
            f"            <stale_log_gap>10</stale_log_gap>\n"
            f"            <reserved_log_items>1</reserved_log_items>{chunk_line}\n"
            f"        </coordination_settings>\n"
            f"\n"
            f"        <raft_configuration>\n"
            + "\n".join(servers) + "\n"
            f"        </raft_configuration>\n"
            f"    </keeper_server>\n"
            f"</clickhouse>\n"
        )

    os.makedirs(configs_dir, exist_ok=True)
    for filenames, hosts, chunk_size, use_s3 in clusters:
        for server_id, filename in enumerate(filenames, start=1):
            path = os.path.join(configs_dir, filename)
            with open(path, "w") as f:
                f.write(make_config(server_id, hosts, chunk_size, use_s3))


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except Exception:
        pass


def fill_test_tree(zk, base, count=300):
    import os as _os
    zk.ensure_path(base)
    for i in range(count):
        zk.create(f"{base}/{i}", _os.urandom(1024))  # random to resist ZSTD compression
    for i in range(0, count, 10):
        zk.delete(f"{base}/{i}")


def cleanup_test_tree(cluster, leader_node, base):
    import helpers.keeper_utils as keeper_utils
    zk = None
    try:
        zk = keeper_utils.get_fake_zk(cluster, leader_node.name)
        if zk.exists(base):
            zk.delete(base, recursive=True)
    except Exception:
        pass
    finally:
        stop_zk(zk)


def verify_test_tree(leader_zk, lagging_zk, base, count=300):
    leader_zk.sync(base)
    lagging_zk.sync(base)
    for i in range(count):
        if i % 10 != 0:
            assert lagging_zk.get(f"{base}/{i}")[0] == leader_zk.get(f"{base}/{i}")[0]
        else:
            assert lagging_zk.exists(f"{base}/{i}") is None


def get_kill_timestamp(node):
    return node.query("SELECT now64(6)").strip()


def _query_text_log(node, after_time, pattern, timeout=15):
    deadline = time.time() + timeout
    while True:
        try:
            node.query("SYSTEM FLUSH LOGS")
            result = node.query(
                f"SELECT message FROM system.text_log "
                f"WHERE event_time_microseconds > '{after_time}' "
                f"AND message LIKE '{pattern}' "
                f"ORDER BY event_time_microseconds"
            ).strip()
            if result:
                return [line for line in result.splitlines() if line]
        except Exception:
            pass

        if time.time() >= deadline:
            return []
        time.sleep(1)


def get_received_snapshot_info(node, after_time, timeout=15):
    lines = _query_text_log(node, after_time, "Saved snapshot % chunks, % bytes)", timeout)
    if not lines:
        return None
    m = re.search(r"Saved snapshot (\d+) \((\d+) chunks, (\d+) bytes\)", lines[-1])
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def get_snapshot_log_lines_for_idx(node, snapshot_log_idx, after_time, timeout=15):
    return _query_text_log(
        node, after_time, f"Saving snapshot {snapshot_log_idx} obj_id %", timeout
    )


def assert_receiving_snapshot_logged(node_lagging, after_time):
    """Assert that `save_logical_snp_obj` fired on the first chunk (proves the snapshot was actually received)."""
    lines = _query_text_log(node_lagging, after_time, "Receiving snapshot % to % disk", timeout=15)
    assert lines, f"Expected 'Receiving snapshot % to % disk' in system.text_log on {node_lagging.name}"


def assert_obj_ids(node_lagging, snapshot_log_idx, expected, after_time):
    lines = get_snapshot_log_lines_for_idx(node_lagging, snapshot_log_idx, after_time)
    assert lines, "No 'Saving snapshot' log lines appeared during recovery"
    all_ids = [int(m.group(1)) for line in lines if (m := re.search(r"obj_id (\d+)", line))]
    duplicates = len(all_ids) - len(set(all_ids))
    # NuRaft may re-send a chunk when a heartbeat fires before the first ACK returns;
    # tolerate at most len(expected)//2 duplicates to catch systematic bugs.
    max_allowed = len(expected) // 2
    assert set(all_ids) == set(expected), f"Expected obj_ids={set(expected)}, got: {sorted(set(all_ids))}"
    assert duplicates <= max_allowed, \
        f"Too many duplicate chunks: {duplicates} (max {max_allowed}), obj_ids={all_ids}"
