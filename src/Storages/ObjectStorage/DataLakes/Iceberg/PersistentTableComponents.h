#pragma once
#include "config.h"

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries
struct PersistentTableComponents
{
    IcebergSchemaProcessorPtr schema_processor;
    IcebergMetadataFilesCachePtr metadata_cache;
    const Int32 format_version;
    const String table_location;
    const CompressionMethod metadata_compression_method;
    const String table_path;
    const std::optional<String> table_uuid;
    const IcebergPathResolver path_resolver;

    /// Invalidate cached metadata for this table under both keys we may have used to cache it
    /// (`table_path` and `table_uuid`). Call this after successfully writing a new metadata file:
    /// a concurrent catalog update may have happened, so we cannot safely cache our own write as
    /// the latest version. By invalidating, the next reader gets the most up-to-date version from
    /// the catalog. Without this, when `iceberg_metadata_staleness_ms` is non-zero, a follow-up
    /// read or `INSERT` may keep using the cached pre-write schema while the writer (which always
    /// reads fresh metadata) sees the post-write schema, leading to a column-count mismatch and an
    /// out-of-bounds access in `DataFileStatistics::getColumnSizes`.
    void invalidateMetadataCache() const
    {
        if (!metadata_cache)
            return;
        metadata_cache->remove(table_path);
        if (table_uuid.has_value())
            metadata_cache->remove(*table_uuid);
    }
};

}

#endif
