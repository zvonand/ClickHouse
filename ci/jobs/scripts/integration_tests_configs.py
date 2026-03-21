import dataclasses
import traceback

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.info import Info


@dataclasses.dataclass
class TC:
    prefix: str
    is_sequential: bool
    comment: str


# Tests that are too slow to run under LLVM coverage instrumentation.
# They either timeout (900s per-test or 7200s session) or cause ClickHouse
# to get stuck during shutdown while writing .profraw coverage data.
LLVM_COVERAGE_SKIP_PREFIXES = [
    "test_storage_s3_queue/test_6.py",
    "test_named_collections_encrypted2/",
    "test_multiple_disks/",
    "test_ytsaurus/",
]

TEST_CONFIGS = [
    TC("test_dns_cache/", True, "no idea why i'm sequential"),
    TC("test_global_overcommit_tracker/", True, "no idea why i'm sequential"),
    TC(
        "test_profile_max_sessions_for_user/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_random_inserts/", True, "no idea why i'm sequential"),
    TC("test_server_overload/", True, "no idea why i'm sequential"),
    TC("test_storage_kafka/", True, "no idea why i'm sequential"),
    TC("test_storage_kerberized_kafka/", True, "no idea why i'm sequential"),
    TC(
        "test_backup_restore_on_cluster/test_concurrency.py",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_storage_iceberg_no_spark/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_with_spark_cache/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_concurrent/", True, "no idea why i'm sequential"),
]

IMAGES_ENV = {
    "clickhouse/dotnet-client": "DOCKER_DOTNET_CLIENT_TAG",
    "clickhouse/integration-helper": "DOCKER_HELPER_TAG",
    "clickhouse/integration-test": "DOCKER_BASE_TAG",
    "clickhouse/kerberos-kdc": "DOCKER_KERBEROS_KDC_TAG",
    "clickhouse/test-mysql80": "DOCKER_TEST_MYSQL80_TAG",
    "clickhouse/test-mysql57": "DOCKER_TEST_MYSQL57_TAG",
    "clickhouse/mysql-golang-client": "DOCKER_MYSQL_GOLANG_CLIENT_TAG",
    "clickhouse/mysql-java-client": "DOCKER_MYSQL_JAVA_CLIENT_TAG",
    "clickhouse/mysql-js-client": "DOCKER_MYSQL_JS_CLIENT_TAG",
    "clickhouse/arrowflight-server-test": "DOCKER_ARROWFLIGHT_SERVER_TAG",
    "clickhouse/mysql-php-client": "DOCKER_MYSQL_PHP_CLIENT_TAG",
    "clickhouse/nginx-dav": "DOCKER_NGINX_DAV_TAG",
    "clickhouse/postgresql-java-client": "DOCKER_POSTGRESQL_JAVA_CLIENT_TAG",
    "clickhouse/python-bottle": "DOCKER_PYTHON_BOTTLE_TAG",
    "clickhouse/integration-test-with-unity-catalog": "DOCKER_BASE_WITH_UNITY_CATALOG_TAG",
    "clickhouse/integration-test-with-hms": "DOCKER_BASE_WITH_HMS_TAG",
    "clickhouse/mysql_dotnet_client": "DOCKER_MYSQL_DOTNET_CLIENT_TAG",
    "clickhouse/s3-proxy": "DOCKER_S3_PROXY_TAG",
}


# collect long-running test suites from CIDB
# clickhouse client --host play.clickhouse.com --secure --user play --query "
# SELECT file, round(sum(med_dur)) AS file_duration_ms
# FROM (
#     SELECT splitByString('::', test_name)[1] AS file, median(test_duration_ms) AS med_dur
#     FROM checks
#     WHERE check_name LIKE 'Integration tests%amd_asan_ubsan%'
#       AND check_start_time >= now() - INTERVAL 14 DAY
#       AND head_ref = 'master' AND startsWith(head_repo, 'ClickHouse/')
#       AND file != '' AND test_status NOT IN ('SKIPPED', 'FAIL')
#     GROUP BY test_name
# )
# GROUP BY file HAVING file_duration_ms >= 100000
# ORDER BY file_duration_ms DESC FORMAT TabSeparated"

RAW_TEST_DURATIONS = """
test_storage_s3_queue/test_6.py	1538520
test_scheduler_cpu_preemptive/test.py	1349570
test_storage_s3_queue/test_5.py	1161329
test_storage_kafka/test_batch_fast.py	1141249
test_storage_delta/test.py	1085863
test_replicated_database/test.py	1080691
test_storage_azure_blob_storage/test.py	972973
test_database_replicated_settings/test.py	943054
test_storage_s3/test.py	857565
test_multiple_disks/test.py	786368
test_ttl_replicated/test.py	771670
test_dictionaries_all_layouts_separate_sources/test_mongo.py	748632
test_backup_restore_new/test.py	681261
test_backup_restore_on_cluster/test_concurrency.py	642060
test_refreshable_mat_view/test.py	627426
test_storage_iceberg_with_spark/test_cluster_table_function.py	618549
test_backup_restore_s3/test.py	606755
test_named_collections/test.py	602227
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	601928
test_storage_s3_queue/test_migration.py	478435
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	478268
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	477059
test_dictionaries_all_layouts_separate_sources/test_mysql.py	475807
test_dictionaries_redis/test.py	463010
test_dictionaries_all_layouts_separate_sources/test_https.py	462339
test_dictionaries_all_layouts_separate_sources/test_http.py	460872
test_ttl_move/test.py	456727
test_async_load_databases/test.py	446296
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	426681
test_storage_s3_queue/test_0.py	412840
test_storage_iceberg_with_spark/test_minmax_pruning.py	406980
test_distributed_load_balancing/test.py	384362
test_storage_s3_queue/test_2.py	380983
test_checking_s3_blobs_paranoid/test.py	365294
test_restore_db_replica/test.py	352690
test_lost_part_during_startup/test.py	352222
test_distributed_ddl/test.py	346430
test_statistics_cache/test.py	343661
test_kafka_bad_messages/test.py	342460
test_log_query_probability/test.py	340109
test_executable_table_function/test.py	336419
test_concurrent_ttl_merges/test.py	335528
test_storage_s3_queue/test_1.py	334635
test_merge_tree_s3/test.py	334024
test_mysql57_database_engine/test.py	329155
test_ytsaurus/test_tables.py	325947
test_mask_sensitive_info/test.py	323296
test_parallel_replicas_insert_select/test.py	309834
test_storage_kafka/test_batch_slow_2.py	303939
test_mysql_database_engine/test.py	301067
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	301003
test_storage_kafka/test_batch_slow_4.py	292787
test_backup_restore_on_cluster/test.py	288161
test_dictionaries_all_layouts_separate_sources/test_file.py	287705
test_refreshable_mv/test.py	287144
test_dns_cache/test.py	287050
test_scheduler/test.py	284613
test_named_collections_encrypted2/test.py	284255
test_drop_is_lock_free/test.py	279611
test_database_delta/test.py	274309
test_storage_iceberg_with_spark/test_expire_snapshots.py	272432
test_storage_kafka/test_batch_slow_1.py	261982
test_storage_kafka/test_batch_slow_5.py	260869
test_dictionaries_dependency/test.py	258080
test_dictionaries_ddl/test.py	256850
test_postgresql_replica_database_engine/test_2.py	250477
test_crash_log/test.py	248388
test_mysql_protocol/test.py	238997
test_row_policy/test.py	235980
test_distributed_directory_monitor_split_batch_on_failure/test.py	231382
test_storage_kafka/test_compression_codec.py	229530
test_storage_kafka/test_batch_slow_6.py	229314
test_postpone_failed_tasks/test.py	219136
test_migration_deduplication_hash/test.py	219055
test_storage_postgresql/test.py	218055
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	215843
test_storage_s3_queue/test_3.py	213101
test_refreshable_mat_view_replicated/test.py	212388
test_postgresql_replica_database_engine/test_3.py	210875
test_storage_iceberg_with_spark/test_partition_pruning.py	206857
test_storage_mongodb/test.py	206842
test_postgresql_replica_database_engine/test_1.py	206440
test_s3_plain_rewritable/test.py	204315
test_backward_compatibility/test_aggregate_function_state.py	197285
test_parallel_replicas_invisible_parts/test.py	196384
test_hedged_requests/test.py	192701
test_postgresql_replica_database_engine/test_0.py	191221
test_filesystem_split_cache/test.py	186692
test_storage_kafka/test_batch_slow_0.py	186298
test_storage_s3_queue/test_4.py	184654
test_postgresql_database_engine/test.py	180576
test_backup_restore_on_cluster/test_cancel_backup.py	173355
test_insert_distributed_async_send/test.py	172319
test_storage_kerberized_kafka/test.py	170940
test_jbod_balancer/test.py	170314
test_keeper_two_nodes_cluster/test.py	170221
test_http_failover/test.py	168479
test_polymorphic_parts/test.py	167319
test_lost_part/test.py	166215
test_backup_restore_new/test_cancel_backup.py	165131
test_implicit_index_upgrade/test.py	164869
test_broken_projections/test.py	162878
test_ytsaurus/test_dictionaries.py	162678
test_storage_iceberg_with_spark/test_position_deletes.py	161917
test_parallel_replicas_over_distributed/test.py	161068
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	160301
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	157454
test_plain_rewritable_backward_compatibility/test.py	155354
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	153332
test_backward_compatibility/test_convert_ordinary.py	152555
test_distributed_frozen_replica/test.py	150657
test_system_logs/test_system_logs.py	147489
test_replicated_mutations/test.py	147108
test_stop_insert_when_disk_close_to_full/test.py	142798
test_library_bridge/test.py	142652
test_merge_tree_azure_blob_storage/test.py	142481
test_dictionaries_update_and_reload/test.py	140901
test_system_clusters_actual_information/test.py	136676
test_quota/test.py	135378
test_http_handlers_config/test.py	135097
test_disk_over_web_server/test.py	135091
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	131362
test_replicated_users/test.py	129576
test_distributed_index_analysis/test.py	129189
test_keeper_zookeeper_converter/test.py	128775
test_storage_iceberg_concurrent/test_concurrent_reads.py	127703
test_replicated_fetches_bandwidth/test.py	127414
test_manipulate_statistics/test.py	126874
test_grant_and_revoke/test_with_table_engine_grant.py	126678
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	125242
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	124641
test_s3_plain_rewritable_rotate_tables/test.py	124004
test_parallel_replicas_custom_key_failover/test.py	122461
test_drop_database_replica/test.py	122292
test_encrypted_disk/test.py	121948
test_distributed_inter_server_secret/test.py	118716
test_restore_replica/test.py	118197
test_role/test.py	116841
test_storage_kafka/test_produce_http_interface.py	116350
test_distributed_ddl_parallel/test.py	115498
test_ddl_worker_replicas/test.py	114280
test_keeper_map/test.py	114093
test_backward_compatibility/test_aggregate_function_state_tuple_return_type.py	113451
test_database_glue/test.py	113117
test_system_logs_recreate/test.py	111257
test_storage_mysql/test.py	110483
test_recompression_ttl/test.py	110002
test_mutations_hardlinks/test.py	108893
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	106969
test_server_reload/test.py	106481
test_refreshable_mv_skip_old_temp_table_ddls/test.py	106453
test_attach_without_fetching/test.py	104045
test_system_merges/test.py	102579
test_keeper_internal_secure/test.py	100812
test_quorum_inserts/test.py	100237
"""


def _parse_raw_durations(raw: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Accept both tab- and space-separated formats; last token is duration
        parts = line.split()
        try:
            duration = int(parts[-1])
        except Exception:
            continue
        path = " ".join(parts[:-1])
        out[path] = duration
    return out


TEST_DURATIONS: dict[str, int] = _parse_raw_durations(RAW_TEST_DURATIONS)


def get_tests_execution_time(info: Info, job_options: str) -> dict[str, int]:
    assert info.updated_at
    start_time_filter = f"parseDateTimeBestEffort('{info.updated_at}')"

    build = job_options.split(",", 1)[0]

    query = f"""
        SELECT
            file,
            round(sum(test_duration_ms)) AS file_duration_ms
        FROM
        (
            SELECT
                splitByString('::', test_name)[1] AS file,
                median(test_duration_ms) AS test_duration_ms
            FROM checks
            WHERE (check_name LIKE 'Integration tests%')
                AND (check_name LIKE '%{build}%')
                AND (check_start_time >= ({start_time_filter} - toIntervalDay(20)))
                AND (check_start_time <= ({start_time_filter} - toIntervalHour(5)))
                AND ((head_ref = 'master') AND startsWith(head_repo, 'ClickHouse/'))
                AND (file != '')
                AND (test_status != 'SKIPPED')
                AND (test_status != 'FAIL')
            GROUP BY test_name
        )
        GROUP BY file
        ORDER BY ALL
        SETTINGS use_query_cache = 1, query_cache_ttl = 432000, query_cache_nondeterministic_function_handling = 'save', query_cache_share_between_users = 1
        FORMAT JSON
    """

    client = CIDBCluster()
    print(query)
    try:
        res = client.do_select_query(query, retries=5, timeout=20)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        return {}

    if not res:
        return {}
    try:
        import json

        data = json.loads(res)
        return {row["file"]: int(row["file_duration_ms"]) for row in data["data"]}
    except Exception as e:
        print(f"ERROR: Failed to parse CIDB response: {e}")
        return {}


def get_optimal_test_batch(
    tests: list[str],
    total_batches: int,
    batch_num: int,
    num_workers: int,
    job_options: str,
    info: Info = None,
) -> tuple[list[str], list[str]]:
    """
    @tests - all tests to run
    @total_batches - total number of batches
    @batch_num - current batch number
    @num_workers - number of parallel workers in a batch
    returns optimal subset of parallel tests for batch_num and optimal subset of sequential tests for batch_num, based on data in TEST_DURATIONS.
    Test files not present in TEST_DURATIONS will be distributed by round robin.
    The function optimizes tail latency of batch with num_workers parallel workers.
    The function works in a deterministic way, so that batch calculated on the other machine with the same input generates the same result.
    """
    # parallel_skip_prefixes sanity check
    for test_config in TEST_CONFIGS:
        assert any(
            test_file.removeprefix("./").startswith(test_config.prefix)
            for test_file in tests
        ), f"No test files found for prefix [{test_config.prefix}] in [{tests}]"

    sequential_test_modules = [
        test_file
        for test_file in tests
        if any(test_file.startswith(test_config.prefix) for test_config in TEST_CONFIGS)
    ]
    parallel_test_modules = [
        test_file for test_file in tests if test_file not in sequential_test_modules
    ]

    if batch_num > total_batches:
        raise ValueError(f"batch_num must be in [1, {total_batches}], got {batch_num}")

    # Helper: group tests by their top-level directory (prefix)
    #  same prefix tests are grouped together to minimize docker pulls in test fixtures in each job batch
    def group_by_prefix(items: list[str]) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for it in sorted(items):
            prefix = it.split("/", 1)[0]
            groups.setdefault(prefix, []).append(it)
        return groups

    # Parallel groups and Sequential groups separated to allow distinct packing
    parallel_groups = group_by_prefix(parallel_test_modules)
    sequential_groups = group_by_prefix(sequential_test_modules)

    durations = TEST_DURATIONS

    # Compute group durations as sum of known test durations within the group
    # TODO: fix in private
    #   ERROR: Failed to get secret [PRIVATE_CI_DB_URL]
    # Do NOT enable this: it makes job setup non-deterministic (distribution of tests among batches differ day-to-day),
    # breaks local reproducibility, and adds an external API dependency that reduces reliability.
    # if info and not info.is_local_run:
    #     durations = get_tests_execution_time(info, job_options)
    #     if not durations:
    #         print("WARNING: CIDB durations not found, using static TEST_DURATIONS")
    #         durations = TEST_DURATIONS

    def groups_with_durations(groups: dict[str, list[str]]):
        known_groups: list[tuple[str, int]] = []  # (prefix, duration)
        unknown_groups: list[str] = []  # prefixes with zero known duration
        for prefix, items in sorted(groups.items()):
            dur = sum(durations.get(t, 0) for t in items)
            if dur > 0:
                known_groups.append((prefix, dur))
            else:
                unknown_groups.append(prefix)
        # Sort known by (-duration, prefix) for deterministic LPT
        known_groups.sort(key=lambda x: (-x[1], x[0]))
        # Sort unknown prefixes to make RR deterministic
        unknown_groups.sort()
        return known_groups, unknown_groups

    p_known, p_unknown = groups_with_durations(parallel_groups)
    s_known, s_unknown = groups_with_durations(sequential_groups)

    # Sequential batches: start from scaled parallel weights to account for worker concurrency
    sequential_batches: list[list[str]] = [[] for _ in range(total_batches)]
    sequential_weights: list[int] = [0] * total_batches

    # LPT assign known-duration sequential groups
    for prefix, dur in s_known:
        idx = min(range(total_batches), key=lambda i: (sequential_weights[i], i))
        # prefix, dur sorted in s_known starting with longest duration - keep the order in batches to decrease tail latency
        sequential_batches[idx].extend(sequential_groups[prefix])
        sequential_weights[idx] += dur

    # Round-robin assign unknown-duration sequential groups
    for i, prefix in enumerate(s_unknown):
        idx = i % total_batches
        sequential_batches[idx].extend(sequential_groups[prefix])

    # Prepare batch containers and weights
    parallel_batches: list[list[str]] = [[] for _ in range(total_batches)]
    parallel_weights: list[int] = [w * num_workers for w in sequential_weights]

    # LPT assign known-duration parallel groups
    for prefix, dur in p_known:
        idx = min(range(total_batches), key=lambda i: (parallel_weights[i], i))
        # prefix, dur sorted in p_known starting with longest duration - keep the order in batches to decrease tail latency
        parallel_batches[idx].extend(parallel_groups[prefix])
        parallel_weights[idx] += dur

    # Sort tests within each batch by duration (longest first) to minimize tail latency
    # when tests are picked by workers from the queue
    for idx in range(total_batches):
        parallel_batches[idx].sort(key=lambda x: (-durations.get(x, 0), x))

    # Round-robin assign unknown-duration parallel groups
    for i, prefix in enumerate(p_unknown):
        idx = i % total_batches
        parallel_batches[idx].extend(parallel_groups[prefix])

    print(
        f"Batches parallel weights: [{[weight // num_workers // 1000 for weight in parallel_weights]}]"
    )

    # Sanity check (non-fatal): ensure total test count preserved
    total_assigned = sum(len(b) for b in parallel_batches) + sum(
        len(b) for b in sequential_batches
    )
    assert total_assigned == len(tests)

    return parallel_batches[batch_num - 1], sequential_batches[batch_num - 1]
