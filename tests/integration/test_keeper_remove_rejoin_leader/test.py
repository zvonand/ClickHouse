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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


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
    Removing the leader triggers a new election; the winner (node4 or node5)
    carries stale peer state from the original cluster and would crash without
    the fix.
    """
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])

    # Identify the current leader and the two followers dynamically so the
    # test does not depend on which node wins the initial election.
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    followers = [n for n in [node1, node2, node3] if n != leader]

    # Step 1: Add node4 to the cluster.
    # node4 creates peer objects for all three existing members.
    # Start Keeper on node4 first so NuRaft can connect immediately when it
    # processes the rcfg command; otherwise the synchronous rcfg call blocks
    # for ~120 s retrying against a non-listening port and times out.
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
    keeper_utils.wait_until_connected(cluster, node4)

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
        keeper_utils.wait_until_connected(cluster, n)

    # Step 3: Add node5 to the cluster.
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
    keeper_utils.wait_until_connected(cluster, node5)

    # Step 4: Remove the second follower.
    # Same `cancel_schedulers()` / `peer::shutdown()` sequence fires.
    follower2 = followers[1]
    result = send_rcfg(
        leader, {"actions": [{"remove_members": [node_id(follower2)]}]}, timeout_sec=30
    )
    assert result["status"] == "ok", f"Failed to remove {follower2.name}: {result}"

    time.sleep(2)

    for n in [leader, node4, node5]:
        keeper_utils.wait_until_connected(cluster, n)

    # Step 5: Add node6 to the cluster.
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
    keeper_utils.wait_until_connected(cluster, node6)

    # Step 6: Remove the original leader.
    # This triggers a new election among {node4, node5, node6}.  The winner
    # (node4 or node5) has stale peer objects whose `hb_task_` was set to null
    # by `peer::shutdown()`.  Without the fix `enable_hb_for_peer()` would
    # dereference that null pointer; with the fix it calls `p.reopen()` first.
    result = send_rcfg(
        leader,
        {"actions": [{"remove_members": [node_id(leader)]}]},
        timeout_sec=30,
    )
    assert result["status"] == "ok", f"Failed to remove original leader: {result}"

    # Verify the cluster recovered with a healthy leader.
    new_leader = keeper_utils.get_leader(cluster, [node4, node5, node6])
    assert new_leader is not None, "No leader elected after removing the original leader"
