#!/usr/bin/env python3
"""
Regression test for NuRaft null pointer dereference when a node becomes leader
after a rolling membership change that leaves abandoned peer objects.

The crash sequence (without the fix):
  1. Start a 3-node cluster (node1, node2, node3) with node1 as leader.
  2. Add node4 to the cluster. node4 creates peer objects for nodes 1, 2, 3.
  3. Remove node2. node2 receives `leave_cluster_req`, setting
     `steps_to_down_ = 2`. After the leader stops heartbeating node2, the
     election timer fires twice, decrementing `steps_to_down_` to 0, which
     triggers `cancel_schedulers()`. `cancel_schedulers()` calls
     `peer::shutdown()` on all of node2's peer objects, setting
     `hb_task_ = null` and `is_shutdown_ = true`.
  4. Add node5 to the cluster.
  5. Remove node3. Same `cancel_schedulers()` / `peer::shutdown()` sequence
     fires on node3's peer objects.
  6. Add node6 to the cluster.
  7. Transfer leadership to node4. `become_leader()` calls
     `enable_hb_for_peer()` for each peer in node4's `peers_` list.
     Without the fix, if any peer has `hb_task_ = null` after `shutdown()`,
     `schedule_task()` dereferences a null pointer and crashes.

The fix (NuRaft PR #91) adds an `is_shutdown_` flag to `peer` and a check in
`enable_hb_for_peer()`: if `p.is_shutdown()`, call `p.reopen()` to recreate
`hb_task_` before scheduling the heartbeat timer.
"""

import json
import os
import shutil
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

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
node4 = cluster.add_instance(
    "node4",
    stay_alive=True,
    with_remote_database_disk=False,
)
node5 = cluster.add_instance(
    "node5",
    stay_alive=True,
    with_remote_database_disk=False,
)
node6 = cluster.add_instance(
    "node6",
    stay_alive=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="function")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()
        if os.path.exists(cluster.instances_dir):
            shutil.rmtree(cluster.instances_dir)


def send_rcfg(node, command, timeout_sec=60):
    result_str = keeper_utils.send_4lw_cmd(
        cluster,
        node,
        cmd="rcfg",
        argument=json.dumps(command),
        timeout_sec=timeout_sec,
    )
    return json.loads(result_str)


def start_keeper(node, config_name):
    """Copy a keeper config into the container and (re)start the keeper server."""
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, config_name),
        "/etc/clickhouse-server/config.d/" + config_name,
    )
    node.start_clickhouse()


def node_id(node):
    """Extract the integer server ID from a node instance (e.g. node3 → 3)."""
    return int(node.name.replace("node", ""))


def test_leader_election_after_rolling_membership_change(started_cluster):
    """
    Regression test: after a rolling membership change (add/remove cycles on
    followers then on the leader), the node that wins the subsequent election
    must not dereference a null pointer inside `enable_hb_for_peer`.

    Reproduces the crash from https://github.com/ClickHouse/NuRaft/pull/91.

    Strategy: replace the two followers first, then replace the leader.
    Transferring leadership to node4 or node5 triggers become_leader(), which
    carries stale peer state from the original cluster and would crash without
    the fix.
    """
    for n in [node1, node2, node3]:
        keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Identify the current leader and the two followers dynamically so the
    # test does not depend on which node wins the initial election.
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    followers = [n for n in [node1, node2, node3] if n != leader]

    # Step 1: Add node4 to the cluster.
    # node4 creates peer objects for all three existing members.
    # node4's config omits use_cluster=false so ClickHouse can connect to the
    # existing Keeper cluster (node1/2/3) during startup, enabling async Keeper
    # initialisation and allowing start_clickhouse() to return before node4
    # reaches quorum.  The rcfg command then adds node4, the leader starts
    # sending heartbeats, and node4's Keeper completes initialisation in the
    # background.
    start_keeper(node4, "enable_keeper4.xml")
    result = send_rcfg(
        leader,
        {
            "actions": [
                {
                    "add_members": [
                        {
                            "id": 4,
                            "endpoint": "node4:9234",
                            "priority": 1,
                        }
                    ]
                }
            ]
        },
        timeout_sec=120,
    )
    assert result["status"] == "ok", f"Failed to add node4: {result}"
    keeper_utils.wait_until_connected(cluster, node4, timeout=60.0)

    # Step 2: Remove the first follower.
    # It receives `leave_cluster_req`, sets `steps_to_down_ = 2`, and after
    # 2 election timeouts calls `cancel_schedulers()`, which calls
    # `peer::shutdown()` on all its peer objects, setting `hb_task_ = null`.
    follower1 = followers[0]
    result = send_rcfg(
        leader, {"actions": [{"remove_members": [node_id(follower1)]}]}, timeout_sec=30
    )
    assert result["status"] == "ok", f"Failed to remove {follower1.name}: {result}"

    # Wait for `cancel_schedulers()` to fire on the removed follower.
    # 4 × election_timeout_upper_bound_ms = 4 × 200 ms = 800 ms; use 2 s.
    time.sleep(2)

    remaining = [n for n in [node1, node2, node3, node4] if n != follower1]
    for n in remaining:
        keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Step 3: Add node5 to the cluster.  node1/3/4 are active at this point,
    # so node5 can connect to them during startup (async Keeper init).
    start_keeper(node5, "enable_keeper5.xml")
    result = send_rcfg(
        leader,
        {
            "actions": [
                {
                    "add_members": [
                        {
                            "id": 5,
                            "endpoint": "node5:9234",
                            "priority": 1,
                        }
                    ]
                }
            ]
        },
        timeout_sec=120,
    )
    assert result["status"] == "ok", f"Failed to add node5: {result}"
    keeper_utils.wait_until_connected(cluster, node5, timeout=60.0)

    # Step 4: Remove the second follower.
    # Same `cancel_schedulers()` / `peer::shutdown()` sequence fires.
    follower2 = followers[1]
    result = send_rcfg(
        leader, {"actions": [{"remove_members": [node_id(follower2)]}]}, timeout_sec=30
    )
    assert result["status"] == "ok", f"Failed to remove {follower2.name}: {result}"

    time.sleep(2)

    for n in [leader, node4, node5]:
        keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Step 5: Add node6 to the cluster.  node1/4/5 are active at this point,
    # so node6 can connect to them during startup (async Keeper init).
    start_keeper(node6, "enable_keeper6.xml")
    result = send_rcfg(
        leader,
        {
            "actions": [
                {
                    "add_members": [
                        {
                            "id": 6,
                            "endpoint": "node6:9234",
                            "priority": 1,
                        }
                    ]
                }
            ]
        },
        timeout_sec=120,
    )
    assert result["status"] == "ok", f"Failed to add node6: {result}"
    keeper_utils.wait_until_connected(cluster, node6, timeout=60.0)

    # Step 6: Replace the original leader.
    # ClickHouse rcfg does not allow removing the current leader directly —
    # transfer_leadership must happen first.  Transferring leadership to node4,
    # node5, or node6 is also exactly what triggers the bug: become_leader()
    # calls enable_hb_for_peer() for every peer, and without the fix the stale
    # hb_task_ = null left by peer::shutdown() causes a null pointer dereference.
    result = send_rcfg(
        leader,
        {
            "actions": [
                {
                    "transfer_leadership": [
                        node_id(node4),
                        node_id(node5),
                        node_id(node6),
                    ]
                }
            ]
        },
        timeout_sec=60,
    )
    assert result["status"] == "ok", f"Failed to transfer leadership: {result}"

    # Find the new leader among the replacement nodes.
    new_leader = keeper_utils.get_leader(cluster, [node4, node5, node6])
    assert new_leader is not None, "No leader elected after leadership transfer"

    # Remove the original leader (now a follower) from the cluster.
    result = send_rcfg(
        new_leader,
        {"actions": [{"remove_members": [node_id(leader)]}]},
        timeout_sec=30,
    )
    assert result["status"] == "ok", f"Failed to remove original leader: {result}"

    # Verify the cluster is healthy with a leader from the replacement nodes.
    final_leader = keeper_utils.get_leader(cluster, [node4, node5, node6])
    assert final_leader is not None, "No leader in final cluster after removing original leader"
