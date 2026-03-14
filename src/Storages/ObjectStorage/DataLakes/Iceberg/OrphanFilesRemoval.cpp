#include "config.h"

#if USE_AVRO

#include <chrono>
#include <set>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/OrphanFilesRemoval.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

namespace
{

/// Collect all files reachable through the metadata graph.
///
/// Traverses: metadata JSON files (from metadata-log), manifest lists (from snapshots),
/// manifest files (from manifest lists), data/delete files (from manifest files),
/// statistics files, and the version-hint file.
std::set<String> collectReachableFiles(
    ObjectStoragePtr object_storage,
    PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log)
{
    std::set<String> reachable;

    auto common_path = persistent_table_components.table_path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    auto [_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context,
        log.get(),
        persistent_table_components.table_uuid);

    auto metadata = getMetadataJSONObject(
        metadata_path,
        object_storage,
        persistent_table_components.metadata_cache,
        context,
        log,
        compression_method,
        persistent_table_components.table_uuid);

    // 1a. Current metadata file
    reachable.insert(metadata_path);

    // 1a. Metadata files from metadata-log
    if (metadata->has(f_metadata_log))
    {
        auto metadata_log = metadata->get(f_metadata_log).extract<Poco::JSON::Array::Ptr>();
        if (metadata_log)
        {
            for (UInt32 i = 0; i < metadata_log->size(); ++i)
            {
                auto entry = metadata_log->getObject(i);
                if (entry->has(f_metadata_file))
                {
                    String mf_path = entry->getValue<String>(f_metadata_file);
                    String storage_path = getProperFilePathFromMetadataInfo(
                        mf_path, persistent_table_components.table_path, persistent_table_components.table_location);
                    reachable.insert(storage_path);
                }
            }
        }
    }

    auto collect_statistics_paths = [&](const char * field_name)
    {
        if (!metadata->has(field_name))
            return;
        auto arr = metadata->get(field_name).extract<Poco::JSON::Array::Ptr>();
        if (!arr)
            return;
        for (UInt32 j = 0; j < arr->size(); ++j)
        {
            auto entry = arr->getObject(j);
            if (entry->has(f_statistics_path))
            {
                String stat_path = entry->getValue<String>(f_statistics_path);
                String storage_path = getProperFilePathFromMetadataInfo(
                    stat_path, persistent_table_components.table_path, persistent_table_components.table_location);
                reachable.insert(storage_path);
            }
        }
    };

    collect_statistics_paths(f_statistics);
    collect_statistics_paths(f_partition_statistics);

    // 1f. Version hint file
    {
        String version_hint = persistent_table_components.table_path;
        if (!version_hint.ends_with('/'))
            version_hint += '/';
        version_hint += "metadata/version-hint.text";
        reachable.insert(version_hint);
    }

    if (!metadata->has(f_snapshots))
    {
        LOG_INFO(log, "No snapshots in metadata, reachable set contains only metadata files");
        return reachable;
    }

    auto snapshots = metadata->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    if (!snapshots || snapshots->size() == 0)
    {
        LOG_INFO(log, "Empty snapshots array, reachable set contains only metadata files");
        return reachable;
    }

    Int32 current_schema_id = metadata->getValue<Int32>(f_current_schema_id);

    // 1b–1d. For each snapshot: manifest list → manifests → data/delete files.
    auto referenced_snapshot_files = collectSnapshotReferencedFiles(
        snapshots, object_storage, persistent_table_components, context, log, current_schema_id);
    reachable.insert(referenced_snapshot_files.manifest_list_storage_paths.begin(), referenced_snapshot_files.manifest_list_storage_paths.end());
    reachable.insert(referenced_snapshot_files.manifest_paths.begin(), referenced_snapshot_files.manifest_paths.end());
    reachable.insert(referenced_snapshot_files.data_file_paths.begin(), referenced_snapshot_files.data_file_paths.end());

    LOG_INFO(log, "Collected {} reachable files from metadata graph", reachable.size());
    return reachable;
}

/// Returns the subset of orphan_paths that were successfully deleted.
std::vector<String> deleteOrphanFiles(
    const std::vector<String> & orphan_paths,
    ObjectStoragePtr object_storage,
    LoggerPtr log)
{
    std::vector<String> deleted_paths;

    for (const auto & path : orphan_paths)
    {
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path));
            LOG_DEBUG(log, "Deleted orphan file {}", path);
            deleted_paths.push_back(path);
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to delete orphan file {}: {}", path, getCurrentExceptionMessage(false));
        }
    }

    return deleted_paths;
}

} // anonymous namespace


RemoveOrphanFilesResult removeOrphanFiles(
    const RemoveOrphanFilesParams & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_table_components)
{
    auto log = getLogger("IcebergRemoveOrphanFiles");

    // Step 1: Collect all reachable files
    auto reachable = collectReachableFiles(
        object_storage, persistent_table_components, data_lake_settings, context, log);

    // Step 2: List all actual files on storage
    String scan_path = persistent_table_components.table_path;
    if (params.location.has_value())
    {
        const String & loc = *params.location;
        if (loc.find("..") != String::npos || loc.starts_with('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "location must be a relative path under the table root, got '{}'", loc);

        if (!scan_path.ends_with('/'))
            scan_path += '/';
        scan_path += loc;
    }

    RelativePathsWithMetadata actual_files;
    object_storage->listObjects(scan_path, actual_files, /* max_keys = */ 0);
    LOG_INFO(log, "Found {} actual files under scan path '{}'", actual_files.size(), scan_path);

    // Step 3: Compute orphans (set difference) with older_than filter
    time_t older_than_threshold = 0;
    if (params.older_than.has_value())
    {
        older_than_threshold = *params.older_than;
    }
    else
    {
        auto now = std::chrono::system_clock::now();
        auto default_age = std::chrono::seconds(3 * 24 * 3600); // 3 days fallback
        auto cutoff = now - default_age;
        older_than_threshold = std::chrono::system_clock::to_time_t(cutoff);
    }

    std::vector<String> orphan_paths;
    Int64 skipped_missing_metadata = 0;

    for (const auto & file_ptr : actual_files)
    {
        const String & path = file_ptr->relative_path;

        if (path.ends_with("version-hint.text"))
            continue;

        if (reachable.contains(path))
            continue;

        if (!file_ptr->metadata.has_value())
        {
            ++skipped_missing_metadata;
            LOG_DEBUG(log, "Skipping file without metadata (no last_modified): {}", path);
            continue;
        }

        auto file_modified = file_ptr->metadata->last_modified.epochTime();
        if (static_cast<time_t>(file_modified) >= older_than_threshold)
            continue;

        LOG_DEBUG(log, "Orphan file: {}", path);
        orphan_paths.push_back(path);
    }

    if (skipped_missing_metadata > 0)
        LOG_WARNING(log, "Skipped {} unreferenced file(s) because last_modified metadata was unavailable; "
            "these files could not be age-checked and were conservatively kept", skipped_missing_metadata);

    LOG_INFO(log, "Found {} orphan files (dry_run={})", orphan_paths.size(), params.dry_run);

    auto tally = [skipped_missing_metadata](const std::vector<String> & paths)
    {
        RemoveOrphanFilesResult r;
        for (const auto & path : paths)
        {
            switch (getFileCategory(path))
            {
                case FileCategory::DATA_FILE:                ++r.deleted_data_files_count; break;
                case FileCategory::POSITION_DELETE_FILE:      ++r.deleted_position_delete_files_count; break;
                case FileCategory::EQUALITY_DELETE_FILE:      ++r.deleted_equality_delete_files_count; break;
                case FileCategory::MANIFEST_FILE:             ++r.deleted_manifest_files_count; break;
                case FileCategory::MANIFEST_LIST:             ++r.deleted_manifest_lists_count; break;
                case FileCategory::METADATA_JSON:             ++r.deleted_metadata_files_count; break;
                case FileCategory::STATISTICS_FILE:           ++r.deleted_statistics_files_count; break;
            }
        }
        r.skipped_missing_metadata_count = skipped_missing_metadata;
        return r;
    };

    if (params.dry_run)
        return tally(orphan_paths);

    if (orphan_paths.empty())
    {
        RemoveOrphanFilesResult r;
        r.skipped_missing_metadata_count = skipped_missing_metadata;
        return r;
    }

    auto deleted_paths = deleteOrphanFiles(orphan_paths, object_storage, log);
    LOG_INFO(log, "Deleted {}/{} orphan files", deleted_paths.size(), orphan_paths.size());

    return tally(deleted_paths);
}

}

#endif
