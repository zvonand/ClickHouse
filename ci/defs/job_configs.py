from praktika import Job
from praktika.utils import Utils

from ci.defs.defs import (
    LLVM_ARTIFACTS_LIST,
    LLVM_FT_NUM_BATCHES,
    LLVM_IT_NUM_BATCHES,
    ArtifactNames,
    BuildTypes,
    JobNames,
    RunnerLabels,
)

LIMITED_MEM = Utils.physical_memory() - 2 * 1024**3
# Keeper stress spins nested Docker inside the integration-tests-runner container.
# Using nearly all host RAM for the outer container can starve the host runner
# and lead to "runner lost communication". Reserve a larger margin on the host
# by capping Keeper to ~70% of physical memory.
KEEPER_DIND_MEM = Utils.physical_memory() * 70 // 100

BINARY_DOCKER_COMMAND = (
    "clickhouse/binary-builder+--network=host"
    f"+--memory={Utils.physical_memory() * 95 // 100}"
    f"+--memory-reservation={Utils.physical_memory() * 9 // 10}"
    f"+--volume=.:/ClickHouse"
)

if Utils.is_arm():
    docker_sock_mount = "--volume=/var/run:/run/host:ro"
else:
    docker_sock_mount = "--volume=/run:/run/host:ro"

build_digest_config = Job.CacheDigestConfig(
    include_paths=[
        "./src",
        "./contrib/",
        "./.gitmodules",
        "./CMakeLists.txt",
        "./PreLoad.cmake",
        "./cmake",
        "./base",
        "./programs",
        "./rust",
        "./ci/jobs/build_clickhouse.py",
        "./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        "./utils/list-licenses",
        "./utils/self-extracting-executable",
    ],
    with_git_submodules=True,
)

fast_test_digest_config = Job.CacheDigestConfig(
    include_paths=[
        "./ci/jobs/fast_test.py",
        "./ci/jobs/scripts/clickhouse_proc.py",
        "./tests/queries/0_stateless/",
        "./tests/config/",
        "./tests/clickhouse-test",
        "./src",
        "./contrib/",
        "./.gitmodules",
        "./CMakeLists.txt",
        "./PreLoad.cmake",
        "./cmake",
        "./base",
        "./programs",
        "./rust",
    ],
)

common_build_job_config = Job.Config(
    name=JobNames.BUILD,
    runs_on=[],  # from parametrize()
    requires=[],
    command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
    run_in_docker=BINARY_DOCKER_COMMAND,
    timeout=3600 * 4,
    digest_config=build_digest_config,
)

common_ft_job_config = Job.Config(
    name=JobNames.STATELESS,
    runs_on=[],  # from parametrize
    command='python3 ./ci/jobs/functional_tests.py --options "{PARAMETER}"',
    # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
    # --cap-add=SYS_PTRACE and --privileged for gdb in docker
    # --root/--privileged/--cgroupns=host is required for clickhouse-test --memory-limit
    run_in_docker=f"clickhouse/stateless-test+--memory={LIMITED_MEM}+--cgroupns=host+--cap-add=SYS_PTRACE+--privileged+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse:mode=1777+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/etc/clickhouse-server1:/etc/clickhouse-server1+--volume=./ci/tmp/etc/clickhouse-server2:/etc/clickhouse-server2+--volume=./ci/tmp/var/log:/var/log+root",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_tests.py",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/functional_tests_results.py",
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
            "./ci/praktika/cidb.py",
            "./tests/queries",
            "./tests/clickhouse-test",
            "./tests/config",
            "./tests/*.txt",
            "./ci/docker/stateless-test",
            "./ci/jobs/scripts/functional_tests/setup_minio.sh",
        ],
    ),
    result_name_for_cidb="Tests",
    timeout=int(3600 * 2.5),
)

common_unit_test_job_config = Job.Config(
    name=JobNames.UNITTEST,
    runs_on=[],  # from parametrize()
    command=f"python3 ./ci/jobs/unit_tests_job.py",
    run_in_docker="clickhouse/test-base+--privileged",
    digest_config=Job.CacheDigestConfig(
        include_paths=["./ci/jobs/unit_tests_job.py"],
    ),
)

common_stress_job_config = Job.Config(
    name=JobNames.STRESS,
    runs_on=[],  # from parametrize()
    command="python3 ./ci/jobs/stress_job.py",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./tests/queries/0_stateless/",
            "./ci/jobs/stress_job.py",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/stress/stress.py",
            "./tests/clickhouse-test",
            "./tests/config",
            "./tests/*.txt",
            "./tests/docker_scripts/",
            "./ci/docker/stress-test",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/log_parser.py",
        ],
    ),
    timeout=3600 * 3,
)
common_integration_test_job_config = Job.Config(
    name=JobNames.INTEGRATION,
    runs_on=[],  # from parametrize
    command="python3 ./ci/jobs/integration_test_job.py --options '{PARAMETER}'",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/integration_test_job.py",
            "./ci/jobs/scripts/integration_tests_configs.py",
            "./tests/integration/",
            "./ci/docker/integration",
            "./ci/jobs/scripts/docker_in_docker.sh",
        ],
    ),
    run_in_docker=f"clickhouse/integration-tests-runner+root+--memory={LIMITED_MEM}+--privileged+--dns-search='.'+--security-opt seccomp=unconfined+--cap-add=SYS_PTRACE+{docker_sock_mount}+--volume=clickhouse_integration_tests_volume:/var/lib/docker+--cgroupns=host",
    post_hooks=["python3 ci/jobs/scripts/job_hooks/docker_volume_clean_up_hook.py"],
)


class JobConfigs:
    style_check = Job.Config(
        name=JobNames.STYLE_CHECK,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/check_style.py",
        run_in_docker="clickhouse/style-test",
        enable_commit_status=True,
    )
    pr_body = Job.Config(
        name=JobNames.PR_BODY,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/pr_formatter_job.py",
        allow_merge_on_failure=True,
        enable_gh_auth=True,
    )
    code_review = Job.Config(
        name=JobNames.CODE_REVIEW,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/copilot_review_job.py --pre",
    )
    ci_results_review = Job.Config(
        name=JobNames.CI_RESULTS_REVIEW,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/copilot_review_job.py --post",
        allow_merge_on_failure=True,
        enable_gh_auth=True,
    )
    _darwin_fast_test_skip = (
        "00004_shard_format_ast_and_remote_table "
        "00005_shard_format_ast_and_remote_table_lambda "
        "00019_shard_quantiles_totals_distributed "
        "00026_shard_something_distributed "
        "00028_shard_big_agg_aj_distributed "
        "00059_shard_global_in "
        "00059_shard_global_in_mergetree "
        "00065_shard_float_literals_formatting "
        "00075_shard_formatting_negate_of_negative_literal "
        "00098_shard_i_union_all "
        "00108_shard_totals_after_having "
        "00112_shard_totals_after_having "
        "00113_shard_group_array "
        "00123_shard_unmerged_result_when_max_distributed_connections_is_one "
        "00124_shard_distributed_with_many_replicas "
        "00154_shard_distributed_with_distinct "
        "00162_shard_global_join "
        "00171_shard_array_of_tuple_remote "
        "00177_inserts_through_http_parts "
        "00184_shard_distributed_group_by_no_merge "
        "00195_shard_union_all_and_global_in "
        "00200_shard_distinct_order_by_limit_distributed "
        "00217_shard_global_subquery_columns_with_same_name "
        "00220_shard_with_totals_in_subquery_remote_and_limit "
        "00223_shard_distributed_aggregation_memory_efficient "
        "00224_shard_distributed_aggregation_memory_efficient_and_overflows "
        "00228_shard_quantiles_deterministic_merge_overflow "
        "00252_shard_global_in_aggregate_function "
        "00257_shard_no_aggregates_and_constant_keys "
        "00266_shard_global_subquery_and_aliases "
        "00274_shard_group_array "
        "00275_shard_quantiles_weighted "
        "00290_shard_aggregation_memory_efficient "
        "00293_shard_max_subquery_depth "
        "00294_shard_enums "
        "00295_global_in_one_shard_rows_before_limit "
        "00305_http_and_readonly "
        "00337_shard_any_heavy "
        "00368_format_option_collision "
        "00372_cors_header "
        "00379_system_processes_port "
        "00409_shard_limit_by "
        "00424_shard_aggregate_functions_of_nullable "
        "00491_shard_distributed_and_aliases_in_where_having "
        "00494_shard_alias_substitution_bug "
        "00506_shard_global_in_union "
        "00515_shard_desc_table_functions_and_subqueries "
        "00534_functions_bad_arguments11 "
        "00534_functions_bad_arguments2 "
        "00550_join_insert_select "
        "00563_insert_into_remote_and_zookeeper_long "
        "00563_shard_insert_into_remote "
        "00573_shard_aggregation_by_empty_set "
        "00588_shard_distributed_prewhere "
        "00604_shard_remote_and_columns_with_defaults "
        "00612_shard_count "
        "00613_shard_distributed_max_execution_time "
        "00614_shard_same_header_for_local_and_remote_node_in_distributed_query "
        "00625_query_in_form_data "
        "00634_logging_shard "
        "00635_shard_distinct_order_by "
        "00636_partition_key_parts_pruning "
        "00673_subquery_prepared_set_performance "
        "00675_shard_remote_with_table_function "
        "00677_shard_any_heavy_merge "
        "00678_shard_funnel_window "
        "00697_in_subquery_shard "
        "00700_decimal_math "
        "00704_drop_truncate_memory_table "
        "00725_quantiles_shard "
        "00850_global_join_dups "
        "00857_global_joinsavel_table_alias "
        "00898_parsing_bad_diagnostic_message "
        "00900_entropy_shard "
        "00942_mutate_index "
        "00943_materialize_index "
        "00961_checksums_in_system_parts_columns_table "
        "00975_sample_prewhere_distributed "
        "00980_shard_aggregation_state_deserialization "
        "00995_exception_while_insert "
        "01018_Distributed__shard_num "
        "01030_limit_by_with_ties_error "
        "01034_prewhere_max_parallel_replicas_distributed "
        "01034_sample_final_distributed "
        "01037_polygon_dicts_correctness_all "
        "01037_polygon_dicts_correctness_fast "
        "01040_distributed_background_insert_batch_inserts "
        "01043_geo_distance "
        "01046_trivial_count_query_distributed "
        "01054_random_printable_ascii_ubsan "
        "01062_max_parser_depth "
        "01071_force_optimize_skip_unused_shards "
        "01072_optimize_skip_unused_shards_const_expr_eval "
        "01077_mutations_index_consistency "
        "01081_PartialSortingTransform_full_column "
        "01085_max_distributed_connections "
        "01098_sum "
        "01104_distributed_numbers_test "
        "01104_distributed_one_test "
        "01160_table_dependencies "
        "01177_group_array_moving "
        "01182_materialized_view_different_structure "
        "01211_optimize_skip_unused_shards_type_mismatch "
        "01223_dist_on_dist "
        "01227_distributed_global_in_issue_2610 "
        "01227_distributed_merge_global_in_primary_key "
        "01244_optimize_distributed_group_by_sharding_key "
        "01245_distributed_group_by_no_merge_with-extremes_and_totals "
        "01245_limit_infinite_sources "
        "01247_optimize_distributed_group_by_sharding_key_dist_on_dist "
        "01253_subquery_in_aggregate_function_JustStranger "
        "01259_combinator_distinct_distributed "
        "01263_type_conversion_nvartolomei "
        "01268_procfs_metrics "
        "01268_shard_avgweighted "
        "01270_optimize_skip_unused_shards_low_cardinality "
        "01288_shard_max_network_bandwidth "
        "01290_max_execution_speed_distributed "
        "01291_distributed_low_cardinality_memory_efficient "
        "01295_aggregation_bug_11413 "
        "01302_polygons_distance "
        "01317_no_password_in_command_line "
        "01319_optimize_skip_unused_shards_nesting "
        "01412_row_from_totals "
        "01417_query_time_in_system_events "
        "01418_query_scope_constants_and_remote "
        "01451_dist_logs "
        "01455_opentelemetry_distributed "
        "01455_shard_leaf_max_rows_bytes_to_read "
        "01473_event_time_microseconds "
        "01509_format_raw_blob "
        "01514_distributed_cancel_query_on_error "
        "01517_select_final_distributed "
        "01521_distributed_query_hang "
        "01528_allow_nondeterministic_optimize_skip_unused_shards "
        "01529_bad_memory_tracking "
        "01548_query_log_query_execution_ms "
        "01532_having_with_totals "
        "01533_quantile_deterministic_assert "
        "01533_sum_if_nullable_bug "
        "01557_max_parallel_replicas_no_sample "
        "01560_merge_distributed_join "
        "01560_ttl_remove_empty_parts "
        "01568_window_functions_distributed "
        "01569_query_profiler_big_query_id "
        "01583_parallel_parsing_exception_with_offset "
        "01584_distributed_buffer_cannot_find_column "
        "01597_columns_list_ignored "
        "01600_quota_by_forwarded_ip "
        "01602_insert_into_table_function_cluster "
        "01602_max_distributed_connections "
        "01636_nullable_fuzz2 "
        "01639_distributed_sync_insert_zero_rows "
        "01640_distributed_async_insert_compression "
        "01646_rewrite_sum_if_bug "
        "01651_bugs_from_15889 "
        "01655_agg_if_nullable "
        "01660_sum_ubsan "
        "01683_dist_INSERT_block_structure_mismatch "
        "01684_insert_specify_shard_id "
        "01685_ssd_cache_dictionary_complex_key "
        "01710_aggregate_projection_with_normalized_states "
        "01710_minmax_count_projection_distributed_query "
        "01710_projections_in_distributed_query "
        "01710_projections_optimize_aggregation_in_order "
        "01710_projections_partial_optimize_aggregation_in_order "
        "01720_country_intersection "
        "01750_parsing_exception "
        "01752_distributed_query_sigsegv "
        "01753_optimize_aggregation_in_order "
        "01754_cluster_all_replicas_shard_num "
        "01755_shard_pruning_with_literal "
        "01756_optimize_skip_unused_shards_rewrite_in "
        "01757_optimize_skip_unused_shards_limit "
        "01758_optimize_skip_unused_shards_once "
        "01780_column_sparse_full "
        "01785_parallel_formatting_memory "
        "01787_map_remote "
        "01790_dist_INSERT_block_structure_mismatch_types_and_names "
        "01791_dist_INSERT_block_structure_mismatch "
        "01834_alias_columns_laziness_filimonov "
        "01851_hedged_connections_external_tables "
        "01854_HTTP_dict_decompression "
        "01860_Distributed__shard_num_GROUP_BY "
        "01872_initial_query_start_time "
        "01882_total_rows_approx "
        "01890_materialized_distributed_join "
        "01892_setting_limit_offset_distributed "
        "01901_in_literal_shard_prune "
        "01915_for_each_crakjie "
        "01922_sum_null_for_remote "
        "01924_argmax_bitmap_state "
        "01927_query_views_log_matview_exceptions "
        "01930_optimize_skip_unused_shards_rewrite_in "
        "01932_null_valid_identifier "
        "01932_remote_sharding_key_column "
        "01940_custom_tld_sharding_key "
        "01943_query_id_check "
        "01948_group_bitmap_and_or_xor_fix "
        "01956_skip_unavailable_shards_excessive_attempts "
        "01961_roaring_memory_tracking "
        "02001_dist_on_dist_WithMergeableStateAfterAggregation "
        "02001_hostname_test "
        "02001_shard_num_shard_count "
        "02002_row_level_filter_bug "
        "02003_WithMergeableStateAfterAggregationAndLimit_LIMIT_BY_LIMIT_OFFSET "
        "02003_memory_limit_in_client "
        "02021_exponential_sum_shard "
        "02022_storage_filelog_one_file "
        "02023_storage_filelog "
        "02023_transform_or_to_in "
        "02025_storage_filelog_virtual_col "
        "02035_isNull_isNotNull_format "
        "02040_clickhouse_benchmark_query_id_pass_through "
        "02047_log_family_data_file_sizes "
        "02050_client_profile_events "
        "02095_function_get_os_kernel_version "
        "02096_totals_global_in_bug "
        "02110_clickhouse_local_custom_tld "
        "02116_clickhouse_stderr "
        "02121_pager "
        "02124_json_each_row_with_progress "
        "02125_fix_storage_filelog "
        "02126_fix_filelog "
        "02133_distributed_queries_formatting "
        "02141_clickhouse_local_interactive_table "
        "02151_http_s_structure_set_eof "
        "02152_bool_type_parsing "
        "02163_shard_num "
        "02165_replicated_grouping_sets "
        "02175_distributed_join_current_database "
        "02176_optimize_aggregation_in_order_empty "
        "02177_merge_optimize_aggregation_in_order "
        "02179_degrees_radians "
        "02183_array_tuple_literals_remote "
        "02183_combinator_if "
        "02203_shebang "
        "02206_format_override "
        "02225_parallel_distributed_insert_select_view "
        "02228_unquoted_dates_in_csv_schema_inference "
        "02233_HTTP_ranged "
        "02250_ON_CLUSTER_grant "
        "02253_empty_part_checksums "
        "02263_format_insert_settings "
        "02281_limit_by_distributed "
        "02293_grouping_function "
        "02310_profile_events_insert "
        "02332_dist_insert_send_logs_level "
        "02341_global_join_cte "
        "02343_group_by_use_nulls_distributed "
        "02344_distinct_limit_distiributed "
        "02346_additional_filters "
        "02346_additional_filters_index "
        "02359_send_logs_source_regexp "
        "02366_union_decimal_conversion "
        "02383_join_and_filtering_set "
        "02420_final_setting_analyzer "
        "02420_stracktrace_debug_symbols "
        "02423_ddl_for_opentelemetry "
        "02454_create_table_with_custom_disk "
        "02457_insert_select_progress_http "
        "02466_distributed_query_profiler "
        "02482_insert_into_dist_race "
        "02483_password_reset "
        "02494_optimize_group_by_function_keys_and_alias_columns "
        "02494_query_cache_http_introspection "
        "02500_remove_redundant_distinct "
        "02501_limits_on_result_for_view "
        "02511_complex_literals_as_aggregate_function_parameters "
        "02521_grouping_sets_plus_memory_efficient_aggr "
        "02525_different_engines_in_temporary_tables "
        "02532_send_logs_level_test "
        "02536_distributed_detach_table "
        "02539_settings_alias "
        "02540_input_format_json_ignore_unknown_keys_in_named_tuple "
        "02550_client_connections_credentials "
        "02560_tuple_format "
        "02566_analyzer_limit_settings_distributed "
        "02571_local_desc_abort_on_twitter_json "
        "02596_build_set_and_remote "
        "02695_logical_optimizer_alias_bug "
        "02703_max_local_write_bandwidth "
        "02704_max_backup_bandwidth "
        "02705_projection_and_ast_optimizations_bug "
        "02761_ddl_initial_query_id "
        "02765_queries_with_subqueries_profile_events "
        "02768_into_outfile_extensions_format "
        "02771_system_user_processes "
        "02785_global_join_too_many_columns "
        "02790_async_queries_in_query_log "
        "02790_optimize_skip_unused_shards_join "
        "02803_remote_cannot_clone_block "
        "02804_clusterAllReplicas_insert "
        "02815_range_dict_no_direct_join "
        "02818_memory_profiler_sample_min_max_allocation_size "
        "02835_fuzz_remove_redundant_sorting "
        "02841_check_table_progress "
        "02844_distributed_virtual_columns "
        "02859_replicated_db_name_zookeeper "
        "02874_array_random_sample "
        "02875_parallel_replicas_cluster_all_replicas "
        "02875_parallel_replicas_remote "
        "02884_async_insert_native_protocol_4 "
        "02887_tuple_element_distributed "
        "02889_parts_columns_filenames "
        "02894_ast_depth_check "
        "02895_peak_memory_usage_http_headers_regression "
        "02900_clickhouse_local_drop_current_database "
        "02901_parallel_replicas_rollup "
        "02907_http_exception_json_bug "
        "02915_analyzer_fuzz_5 "
        "02916_local_insert_into_function "
        "02933_replicated_database_forbid_create_as_select "
        "02935_http_content_type_with_http_headers_progress "
        "02950_dictionary_ssd_cache_short_circuit "
        "02954_analyzer_fuzz_i57086 "
        "02961_drop_tables "
        "02962_analyzer_resolve_group_by_on_shards "
        "02967_analyzer_fuzz "
        "02968_file_log_multiple_read "
        "02971_analyzer_remote_id "
        "02971_limit_by_distributed "
        "02984_topk_empty_merge "
        "02985_shard_query_start_time "
        "02992_analyzer_group_by_const "
        "02998_http_redirects "
        "02999_scalar_subqueries_bug_1 "
        "03001_insert_threads_deduplication "
        "03008_deduplication_remote_insert_select "
        "03010_virtual_memory_mappings_asynchronous_metrics "
        "03012_prewhere_merge_distributed "
        "03013_forbid_attach_table_if_active_replica_already_exists "
        "03020_long_values_pretty_are_not_cut_if_single "
        "03021_get_client_http_header "
        "03025_clickhouse_host_env "
        "03031_filter_float64_logical_error "
        "03035_max_insert_threads_support "
        "03095_group_by_server_constants_bug "
        "03096_text_log_format_string_args_not_empty "
        "03113_analyzer_not_found_column_in_block_2 "
        "03114_analyzer_cte_with_join "
        "03133_help_message_verbosity "
        "03143_group_by_constant_secondary "
        "03147_rows_before_limit_fix "
        "03154_recursive_cte_distributed "
        "03150_trace_log_add_build_id "
        "03155_analyzer_interpolate "
        "03156_analyzer_array_join_distributed "
        "03160_pretty_format_tty "
        "03164_analyzer_global_in_alias "
        "03165_storage_merge_view_prewhere "
        "03172_system_detached_tables "
        "03196_local_memory_limit "
        "03204_distributed_with_scalar_subquery "
        "03208_buffer_over_distributed_type_mismatch "
        "03213_distributed_analyzer "
        "03214_parsing_archive_name_file "
        "03215_analyzer_materialized_constants_bug "
        "03217_datetime64_constant_to_ast "
        "03221_merge_profile_events "
        "03228_clickhouse_local_copy_argument "
        "03229_query_condition_cache_folded_constants "
        "03240_insert_select_named_tuple "
        "03243_cluster_not_found_column "
        "03248_with_insert_with "
        "03258_nonexistent_db "
        "03259_grouping_sets_aliases "
        "03269_bf16 "
        "03271_benchmark_metrics "
        "03271_parse_sparse_columns_defaults "
        "03275_basic_auth_interactive "
        "03277_analyzer_array_join_fix "
        "03287_dynamic_and_json_squashing_fix "
        "03302_analyzer_distributed_filter_push_down "
        "03305_mergine_aggregated_filter_push_down "
        "03305_parallel_with_query_log "
        "03303_distributed_explain "
        "03306_expose_headers "
        "03316_analyzer_unique_table_aliases_dist "
        "03321_forwarded_for "
        "03322_initial_query_start_time_check "
        "03328_syntax_error_exception "
        "03360_bool_remote "
        "03362_basic_auth_interactive_not_with_authorization_never "
        "03362_merge_tree_with_background_refresh "
        "03364_estimate_compression_ratio "
        "03369_predicate_pushdown_enforce_literal_type "
        "03369_variant_escape_filename_merge_tree "
        "03371_analyzer_filter_pushdown_distributed "
        "03371_constant_alias_columns "
        "03371_dynamic_values_parsing_templates "
        "03381_file_log_merge_empty "
        "03381_remote_constants "
        "03381_udf_asterisk "
        "03394_pr_insert_select "
        "03397_disallow_empty_session_id "
        "03400_distributed_final "
        "03400_explain_distributed_bug "
        "03405_ssd_cache_incorrect_min_max_lifetimes_and_block_size "
        "03408_limit_by_rows_before_limit_dist "
        "03448_analyzer_array_join_alias_in_join_using_bug "
        "03448_window_functions_distinct_distributed "
        "03459_socket_asynchronous_metrics "
        "03454_global_join_index_subqueries "
        "03457_numeric_indexed_vector_build "
        "03513_fix_shard_num_column_to_function_pass_with_nulls "
        "03519_analyzer_tuple_cast "
        "03519_cte_allow_push_predicate_ast_for_distributed_subqueries_bug "
        "03520_analyzer_distributed_in_cte_bug "
        "03526_columns_substreams_in_wide_parts "
        "03536_ch_as_client_and_local "
        "03527_format_insert_partition "
        "03533_lexer_c_library "
        "03537_clickhouse_local_drop_view_sync_temp_mode "
        "03545_progress_header_goes_first "
        "03546_json_input_output_map_as_array "
        "03554_connection_crash "
        "03562_parallel_replicas_remote_with_cluster "
        "03572_pr_remote_in_subquery "
        "03577_server_constant_folding "
        "03593_prewhere_bytes_read_stat_bug "
        "03593_remote_map_in "
        "03595_pread_threadpool_direct_io "
        "03620_analyzer_distributed_global_in "
        "03620_distributed_index_analysis "
        "03620_mergeTreeAnalyzeIndexesUUID "
        "03622_explain_indexes_distributed_index_analysis "
        "03636_benchmark_error_messages "
        "03653_benchmark_proto_caps_option "
        "03658_negative_limit_offset_distributed "
        "03681_distributed_fractional_limit_offset "
        "03701_distributed_index_analysis_async_modes "
        "03702_geometry_functions "
        "03703_prelimit_explain_message "
        "03710_array_join_in_map_bug "
        "03713_replicated_columns_in_external_data_bug "
        "03726_distributed_alias_column_order "
        "03733_pr_view_filter_pushdown "
        "03746_buffers_input_output_format_misc "
        "03757_optimize_skip_unused_shards_with_type_cast "
        "03778_print_time_initial_query "
        "03783_serialize_into_sparse_with_subcolumn_extraction "
        "03786_AST_formatting_inconsistencies_in_debug_check "
        "03787_no_excessive_output_on_syntax_error "
        "03794_global_in_nullable_type_mismatch "
        "03811_negative_limit_offset_distributed_large "
        "03821_remote_grouping_set_aggregation_keys_assert "
        "03836_distributed_index_analysis_pk_expression "
        "03836_distributed_index_analysis_skip_index_expression "
        "03911_fractional_limit_offset_distributed_large "
        "03928_loop_row_policy "
        "04001_materialized_cte_distributed "
        "04010_describe_remote_rbac_bypass "
        "04029_distributed_index_analysis_sampling "
        "04036_materialized_cte_distributed_race "
        "04037_obfuscator_dictionary_definitions "
        "04041_materialized_cte_parallel_replicas "
        "04043_materialized_cte_serialize_query_plan "
        "04049_dictionary_local_create_with_bogus_function "
        "04052_distributed_index_analysis_in_subquery_no_quadratic "
        "04054_backup_restore_validate_entry_paths "
        "04056_execute_as_format "
        "04056_npy_large_shape_validation "
        "04061_trivial_count_aggregate_function_argument_types_distributed"
    )
    fast_test = Job.Config(
        name=JobNames.FAST_TEST,
        runs_on=RunnerLabels.AMD_LARGE,
        command="python3 ./ci/jobs/fast_test.py",
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker="clickhouse/fasttest+--network=host+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/var/log:/var/log+--volume=.:/ClickHouse",
        digest_config=fast_test_digest_config,
        result_name_for_cidb="Tests",
    )
    darwin_fast_test_jobs = Job.Config(
        name="Darwin fast test",
        runs_on=None,  # from parametrize()
        command=f"python3 ./ci/jobs/fast_test.py --set-status-success --skip {_darwin_fast_test_skip}",
        digest_config=fast_test_digest_config,
        result_name_for_cidb="Darwin tests",
        allow_merge_on_failure=True,
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            runs_on=RunnerLabels.MACOS_ARM_SMALL,
            requires=[ArtifactNames.CH_ARM_DARWIN_BIN],
        ),
    )
    smoke_tests_macos = Job.Config(
        name=JobNames.SMOKE_TEST_MACOS,
        runs_on=RunnerLabels.MACOS_AMD_SMALL,
        command="python3 ./ci/jobs/smoke_test.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/smoke_test.py",
            ],
        ),
        requires=[ArtifactNames.CH_AMD_DARWIN_BIN],
    )
    tidy_build_arm_jobs = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.ARM_TIDY,
            provides=[],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    tidy_build_amd_jobs = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_TIDY,
            provides=[],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DEBUG,
            provides=[ArtifactNames.CH_AMD_DEBUG, ArtifactNames.DEB_AMD_DEBUG],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_ASAN_UBSAN,
            provides=[
                ArtifactNames.CH_AMD_ASAN_UBSAN,
                ArtifactNames.DEB_AMD_ASAN_UBSAN,
                ArtifactNames.UNITTEST_AMD_ASAN_UBSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_TSAN,
            provides=[
                ArtifactNames.CH_AMD_TSAN,
                ArtifactNames.DEB_AMD_TSAN,
                ArtifactNames.UNITTEST_AMD_TSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MSAN,
            provides=[
                ArtifactNames.CH_AMD_MSAN,
                ArtifactNames.DEB_AMD_MSAN,
                ArtifactNames.UNITTEST_AMD_MSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_BINARY,
            provides=[ArtifactNames.CH_AMD_BINARY],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DEBUG,
            provides=[ArtifactNames.CH_ARM_DEBUG, ArtifactNames.DEB_ARM_DEBUG],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_ASAN_UBSAN,
            provides=[
                ArtifactNames.CH_ARM_ASAN_UBSAN,
                ArtifactNames.DEB_ARM_ASAN_UBSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_TSAN,
            provides=[
                ArtifactNames.CH_ARM_TSAN,
                ArtifactNames.DEB_ARM_TSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_MSAN,
            provides=[ArtifactNames.CH_ARM_MSAN, ArtifactNames.DEB_ARM_MSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_UBSAN,
            provides=[ArtifactNames.CH_ARM_UBSAN, ArtifactNames.DEB_ARM_UBSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_BINARY,
            provides=[
                ArtifactNames.CH_ARM_BINARY,
                ArtifactNames.PARSER_MEMORY_PROFILER,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    coverage_build_jobs = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.LLVM_COVERAGE_BUILD,
            provides=[
                ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
            ],
            runs_on=RunnerLabels.AMD_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.PER_TEST_COVERAGE,
            provides=[
                ArtifactNames.CH_AMD_PER_TEST_COVERAGE_BUILD,
            ],
            runs_on=RunnerLabels.AMD_LARGE,
        ),
    )
    release_build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_RELEASE,
            provides=[
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
            timeout=3 * 3600,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_RELEASE,
            provides=[
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    extra_validation_build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        # Job.ParamSet(
        #     parameter=BuildTypes.ARM_TSAN,
        #     provides=[
        #         ArtifactNames.CH_ARM_TSAN,
        #     ],
        #     runs_on=RunnerLabels.ARM_LARGE,
        # ),
    )
    special_build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DARWIN,
            provides=[ArtifactNames.CH_AMD_DARWIN_BIN],
            runs_on=RunnerLabels.AMD_LARGE,  # cannot crosscompile on arm
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            provides=[ArtifactNames.CH_ARM_DARWIN_BIN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_V80COMPAT,
            provides=[ArtifactNames.CH_ARM_V80COMPAT],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_FREEBSD,
            provides=[ArtifactNames.CH_AMD_FREEBSD],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.PPC64LE,
            provides=[ArtifactNames.CH_PPC64LE],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_COMPAT,
            provides=[ArtifactNames.CH_AMD_COMPAT],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MUSL,
            provides=[ArtifactNames.CH_AMD_MUSL],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.RISCV64,
            provides=[ArtifactNames.CH_RISCV64],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.S390X,
            provides=[ArtifactNames.CH_S390X],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.LOONGARCH64,
            provides=[ArtifactNames.CH_LOONGARCH64],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_FUZZERS,
            provides=[],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    install_check_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/install_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/install_check.py",
                "./ci/docker/install",
            ],
        ),
        timeout=900,
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            requires=[
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
            ],
        ),
    )
    install_check_master_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/install_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/install_check.py",
                "./ci/docker/install",
            ],
        ),
        timeout=900,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
            ],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            requires=[
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
                ArtifactNames.CH_ARM_RELEASE,
            ],
        ),
    )
    stateless_tests_flaky_pr_jobs = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="arm_asan_ubsan, flaky check",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_asan_ubsan, flaky check",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan, flaky check",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan, flaky check",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="amd_debug, flaky check",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
    )
    # --root/--privileged/--cgroupns=host is required for clickhouse-test --memory-limit
    bugfix_validation_ft_pr_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE_FT,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/functional_tests.py --options BugfixValidation",
        # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
        run_in_docker="clickhouse/stateless-test+--network=host+--privileged+--cgroupns=host+root+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse:mode=1777",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/functional_tests.py",
                "./tests/queries",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
            ],
        ),
        result_name_for_cidb="Tests",
    )
    lightweight_functional_tests_job = Job.Config(
        name="Quick functional tests",
        command="python3 ./ci/jobs/clickhouse_light.py --path ./ci/tmp/clickhouse",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/clickhouse_light.py",
                "./ci/jobs/queries",
            ],
        ),
        requires=[ArtifactNames.CH_AMD_DEBUG],
        runs_on=RunnerLabels.AMD_SMALL,
    )
    functional_tests_jobs = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, distributed plan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM_CPU,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="amd_asan_ubsan, db disk, distributed plan, sequential",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, old analyzer, s3 storage, DatabaseReplicated, WasmEdge, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_old_s3_db_repl_wasm_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, old analyzer, s3 storage, DatabaseReplicated, WasmEdge, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_old_s3_db_repl_wasm_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, ParallelReplicas, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, ParallelReplicas, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, AsyncInsert, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_async_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, AsyncInsert, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_async_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_debug, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM_CPU,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_LARGE,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, WasmEdge, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_LARGE,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, WasmEdge, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL_MEM,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="amd_debug, distributed plan, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, distributed plan, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, s3 storage, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, s3 storage, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL_MEM,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="arm_binary, parallel",
            runs_on=RunnerLabels.ARM_MEDIUM_CPU,
            requires=[ArtifactNames.CH_ARM_BINARY],
        ),
        Job.ParamSet(
            parameter="arm_binary, sequential",
            runs_on=RunnerLabels.ARM_SMALL,
            requires=[ArtifactNames.CH_ARM_BINARY],
        ),
    )
    functional_tests_jobs_coverage = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"{BuildTypes.PER_TEST_COVERAGE}, per_test_coverage, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL,
                requires=[ArtifactNames.CH_AMD_PER_TEST_COVERAGE_BUILD],
            )
            for total_batches in (8,)
            for batch in range(1, total_batches + 1)
        ]
    )
    functional_tests_jobs_azure = common_ft_job_config.set_allow_merge_on_failure(
        True
    ).parametrize(
        Job.ParamSet(
            parameter="arm_asan_ubsan, azure, parallel",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan, azure, sequential",
            runs_on=RunnerLabels.ARM_SMALL_MEM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    bugfix_validation_it_job = (
        common_integration_test_job_config.set_name(JobNames.BUGFIX_VALIDATE_IT)
        .set_runs_on(RunnerLabels.AMD_SMALL_MEM)
        .set_command(
            "python3 ./ci/jobs/integration_test_job.py --options BugfixValidation"
        )
    )
    unittest_jobs = common_unit_test_job_config.parametrize(
        Job.ParamSet(
            parameter="asan_ubsan",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="tsan",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="msan",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_MSAN],
        ),
    )
    stress_test_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_debug",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan, s3",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_TSAN],
        ),
        Job.ParamSet(
            parameter="arm_msan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_MSAN],
        ),
        Job.ParamSet(
            parameter="arm_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_UBSAN],
        ),
    )
    # might be heavy on azure - run only on master
    stress_test_azure_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="azure, amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="azure, amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_TSAN],
        ),
    )
    upgrade_test_jobs = Job.Config(
        name=JobNames.UPGRADE,
        runs_on=["from param"],
        command="python3 ./ci/jobs/upgrade_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/upgrade_job.py",
                "./ci/jobs/stress_job.py",
                "./ci/jobs/scripts/stress/stress.py",
                "./tests/docker_scripts/",
                "./ci/docker/stress-test",
                "./ci/jobs/scripts/log_parser.py",
            ]
        ),
        timeout=3600 * 2,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_RELEASE],
        ),
    )
    # why it's master only?
    integration_test_asan_master_jobs = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, db disk, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ]
    )
    integration_test_jobs_required = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, db disk, old analyzer, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_binary, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_BINARY],
            )
            for total_batches in (5,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"arm_binary, distributed plan, {batch}/{total_batches}",
                runs_on=RunnerLabels.ARM_MEDIUM,
                requires=[ArtifactNames.CH_ARM_BINARY],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ],
    )
    integration_test_jobs_non_required = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
    )
    integration_test_asan_flaky_pr_jobs = (
        common_integration_test_job_config.parametrize(
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, flaky",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
        )
    )

    build_llvm_coverage_job = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.LLVM_COVERAGE_BUILD,
            provides=[
                ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
                ArtifactNames.UNITTEST_LLVM_COVERAGE,
            ],
            runs_on=RunnerLabels.AMD_LARGE,
        ),
    )

    unittest_llvm_coverage_job = common_unit_test_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_llvm_coverage",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_LLVM_COVERAGE],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE],
        ),
    )

    functional_test_llvm_coverage_jobs = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_llvm_coverage, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
                provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_{batch}"],
            )
            for total_batches in (LLVM_FT_NUM_BATCHES,)
            for batch in range(1, total_batches + 1)
        ]
    )

    integration_test_llvm_coverage_jobs = (
        common_integration_test_job_config.parametrize(
            *[
                Job.ParamSet(
                    parameter=f"amd_llvm_coverage, {batch}/{total_batches}",
                    runs_on=RunnerLabels.AMD_MEDIUM,
                    requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
                    provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_it_{batch}"],
                )
                for total_batches in (LLVM_IT_NUM_BATCHES,)
                for batch in range(1, total_batches + 1)
            ],
        )
    )

    integration_test_targeted_pr_jobs = common_integration_test_job_config.parametrize(
        Job.ParamSet(
            parameter=f"amd_asan_ubsan, targeted",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        )
    )
    # Keeper stress job config — shared by PR and nightly workflows.
    # Mode (PR vs nightly faults vs nightly no-faults) is determined inside the job
    # script via Info().pr_number and Info().workflow_name.
    keeper_stress_job = Job.Config(
        name="Keeper Stress",
        runs_on=RunnerLabels.ARM_LARGE,
        command="python3 ./ci/jobs/keeper_stress_job.py",
        run_in_docker=(
            f"clickhouse/integration-tests-runner+root+--memory={KEEPER_DIND_MEM}+--privileged+--dns-search='.'+"
            f"--security-opt seccomp=unconfined+--cap-add=SYS_PTRACE+{docker_sock_mount}+--volume=clickhouse_integration_tests_volume:/var/lib/docker+--ulimit nofile=262144:262144"
        ),
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/keeper_stress_job.py",
                "./ci/jobs/scripts/docker_in_docker.sh",
                "./tests/stress/keeper/",
                "./tests/integration/helpers/",
                "./src/Coordination/",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_BINARY],
        result_name_for_cidb="Keeper Stress",
        timeout=24 * 3600,
        post_hooks=["python3 ./ci/jobs/scripts/ingest_keeper_metrics.py"],
    )
    compatibility_test_jobs = Job.Config(
        name=JobNames.COMPATIBILITY,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/compatibility_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/compatibility_check.py",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[ArtifactNames.DEB_AMD_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            requires=[ArtifactNames.DEB_ARM_RELEASE],
        ),
    )
    ast_fuzzer_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command=f"python3 ./ci/jobs/ast_fuzzer_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
    )
    ast_fuzzer_targeted_pr_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/ast_fuzzer_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/find_symbols.py",
                "./ci/jobs/scripts/find_tests.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug, targeted",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, targeted, old_compatibility",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),

    )
    buzz_fuzzer_jobs = Job.Config(
        name=JobNames.BUZZHOUSE,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/buzzhouse_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/buzzhouse_job.py",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
    )
    performance_comparison_with_master_head_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["#from param"],
        command='python3 ./ci/jobs/performance_tests.py --test-options "{PARAMETER}"',
        # TODO: switch to stateless-test image
        run_in_docker="clickhouse/performance-comparison",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/performance/",
                "./ci/jobs/scripts/perf/",
                "./ci/jobs/performance_tests.py",
                "./ci/docker/performance-comparison",
            ],
        ),
        timeout=2 * 3600,
        result_name_for_cidb="Tests",
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_release, master_head, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_RELEASE],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"arm_release, master_head, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_ARM,
                requires=[ArtifactNames.CH_ARM_RELEASE],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
    )
    performance_comparison_with_release_base_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["#from param"],
        command='python3 ./ci/jobs/performance_tests.py --test-options "{PARAMETER}"',
        # TODO: switch to stateless-test image
        run_in_docker="clickhouse/performance-comparison",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/performance/",
                "./ci/jobs/scripts/perf/",
                "./ci/jobs/performance_tests.py",
                "./ci/docker/performance-comparison",
            ],
        ),
        timeout=2 * 3600,
        result_name_for_cidb="Tests",
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"arm_release, release_base, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_ARM,
                requires=[ArtifactNames.CH_ARM_RELEASE],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ]
    )
    clickbench_master_jobs = Job.Config(
        name=JobNames.CLICKBENCH,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="python3 ./ci/jobs/clickbench.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/clickbench.py",
                "./ci/jobs/scripts/clickbench/",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
            ],
        ),
        run_in_docker="clickhouse/stateless-test+--shm-size=16g+--network=host",
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_RELEASE,
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_RELEASE],
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_RELEASE,
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_RELEASE],
        ),
    )
    docs_job = Job.Config(
        name=JobNames.DOCS,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "**/*.md",
                "./docs",
                "./ci/jobs/docs_job.py",
                "CHANGELOG.md",
                "./src/Functions",
            ],
        ),
        run_in_docker="clickhouse/docs-builder",
        requires=[JobNames.STYLE_CHECK, ArtifactNames.CH_ARM_BINARY],
    )
    docs_job_mintlify = Job.Config(
        name=JobNames.DOCS_MINTLIFY,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job_mintlify.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./docs/docs",
            ],
            exclude_paths=[
                "./docs/en/",
                "./changelogs/"
            ],
        ),
        run_in_docker="clickhouse/docs-builder"
    )
    docker_server = Job.Config(
        name=JobNames.DOCKER_SERVER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/docker_server.py --tag-type head --allow-build-reuse",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docker_server.py",
                "./docker/server",
                "./docker/keeper",
            ],
        ),
        requires=["Build (amd_release)", "Build (arm_release)"],
        needs_jobs_from_requires=True,
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    )
    docker_keeper = Job.Config(
        name=JobNames.DOCKER_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/docker_server.py --tag-type head --allow-build-reuse",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docker_server.py",
                "./docker/server",
                "./docker/keeper",
            ],
        ),
        requires=["Build (amd_release)", "Build (arm_release)"],
        needs_jobs_from_requires=True,
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    )
    sqlancer_master_jobs = Job.Config(
        name=JobNames.SQLANCER,
        runs_on=[],  # from parametrize()
        command="./ci/jobs/sqlancer_job.sh",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/sqlancer_job.sh", "./ci/docker/sqlancer-test"],
        ),
        run_in_docker="clickhouse/sqlancer-test",
        timeout=3600,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
    )
    sqltest_master_job = Job.Config(
        name=JobNames.SQL_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/sqltest_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqltest_job.py",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    sqllogic_test_master_job = Job.Config(
        name=JobNames.SQL_LOGIC_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/sqllogic_test.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqllogic_test.py",
                "./tests/sqllogic/",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    jepsen_keeper = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/jepsen_check.py keeper",
        requires=["Build (amd_binary)"],
    )
    jepsen_server = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/jepsen_check.py server",
        requires=["Build (amd_binary)"],
    )
    libfuzzer_job = Job.Config(
        name=JobNames.LIBFUZZER_TEST,
        runs_on=RunnerLabels.ARM_MEDIUM,
        command="python3 ./ci/jobs/libfuzzer_test_check.py 'libFuzzer tests'",
        requires=[ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS],
    )
    toolchain_build_jobs = Job.Config(
        name=JobNames.BUILD_TOOLCHAIN,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/build_toolchain.py",
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=8 * 3600,
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/build_toolchain.py"],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd64",
            runs_on=RunnerLabels.AMD_LARGE,
            provides=[ArtifactNames.TOOLCHAIN_PGO_BOLT_AMD],
        ),
        Job.ParamSet(
            parameter="aarch64",
            runs_on=RunnerLabels.ARM_LARGE,
            provides=[ArtifactNames.TOOLCHAIN_PGO_BOLT_ARM],
        ),
    )
    update_toolchain_dockerfile_job = Job.Config(
        name=JobNames.UPDATE_TOOLCHAIN_DOCKERFILE,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/update_toolchain_dockerfile.py",
        enable_gh_auth=True,
    )
    vector_search_stress_job = Job.Config(
        name="Vector Search Stress",
        runs_on=RunnerLabels.ARM_MEDIUM,
        run_in_docker="clickhouse/performance-comparison",
        command="python3 ./ci/jobs/vector_search_stress_tests.py",
    )
    llvm_coverage_job = Job.Config(
        name=JobNames.LLVM_COVERAGE,
        runs_on=RunnerLabels.AMD_SMALL,
        run_in_docker="clickhouse/test-base",
        requires=[
            ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
            ArtifactNames.UNITTEST_LLVM_COVERAGE,
            *LLVM_ARTIFACTS_LIST,
        ],
        provides=[
            ArtifactNames.LLVM_COVERAGE_INFO_FILE,
        ],
        command="python3 ./ci/jobs/llvm_coverage_job.py",
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/llvm_coverage_hook.py"],
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/llvm_coverage_job.py"],
        ),
        timeout=3600,
        enable_gh_auth=True,
    )
