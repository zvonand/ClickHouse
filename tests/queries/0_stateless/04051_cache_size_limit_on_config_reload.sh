#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, long

# Test that cache sizes are properly limited by cache_size_to_ram_max_ratio * RAM
# when config is reloaded with oversized cache settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_dir="${CLICKHOUSE_CONFIG_DIR}/config.d"
tmp_config="${config_dir}/test_04051_cache_size.xml"

# Much larger than any real machine's RAM
oversized_cache_size=1098437885952000  # 999 TiB

# Small value to verify that config reload actually works
small_cache_size=5242880  # 5 MiB

apply_cache_config()
{
    local size=$1
    cat > "${tmp_config}" << EOF
<clickhouse>
    <uncompressed_cache_size>${size}</uncompressed_cache_size>
    <mark_cache_size>${size}</mark_cache_size>
    <primary_index_cache_size>${size}</primary_index_cache_size>
    <index_uncompressed_cache_size>${size}</index_uncompressed_cache_size>
    <index_mark_cache_size>${size}</index_mark_cache_size>
    <vector_similarity_index_cache_size>${size}</vector_similarity_index_cache_size>
    <text_index_tokens_cache_size>${size}</text_index_tokens_cache_size>
    <text_index_header_cache_size>${size}</text_index_header_cache_size>
    <text_index_postings_cache_size>${size}</text_index_postings_cache_size>
    <mmap_cache_size>${size}</mmap_cache_size>
    <query_condition_cache_size>${size}</query_condition_cache_size>
    <query_cache>
        <max_size_in_bytes>${size}</max_size_in_bytes>
    </query_cache>
</clickhouse>
EOF
    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use' || true
}

# Apply a small cache size and verify it was applied.
apply_cache_config "${small_cache_size}"

$CLICKHOUSE_CLIENT --query "
SELECT name, toUInt64(value) = ${small_cache_size} AS is_small
FROM system.server_settings
WHERE name IN (
    'uncompressed_cache_size',
    'mark_cache_size',
    'primary_index_cache_size',
    'index_uncompressed_cache_size',
    'index_mark_cache_size',
    'vector_similarity_index_cache_size',
    'text_index_tokens_cache_size',
    'text_index_header_cache_size',
    'text_index_postings_cache_size',
    'mmap_cache_size',
    'query_condition_cache_size',
    'query_cache.max_size_in_bytes'
)
ORDER BY name
"

# Apply an oversized cache size and verify that capping works.
# The actual cache sizes must be capped at cache_size_to_ram_max_ratio * RAM,
apply_cache_config "${oversized_cache_size}"

$CLICKHOUSE_CLIENT --query "
SELECT name,
    toUInt64(value) < ${oversized_cache_size} AS is_capped,
    toUInt64(value) > ${small_cache_size} AS grew_from_small
FROM system.server_settings
WHERE name IN (
    'uncompressed_cache_size',
    'mark_cache_size',
    'primary_index_cache_size',
    'index_uncompressed_cache_size',
    'index_mark_cache_size',
    'vector_similarity_index_cache_size',
    'text_index_tokens_cache_size',
    'text_index_header_cache_size',
    'text_index_postings_cache_size',
    'mmap_cache_size',
    'query_condition_cache_size',
    'query_cache.max_size_in_bytes'
)
ORDER BY name
"

# Cleanup
rm -f "${tmp_config}"
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use' || true
