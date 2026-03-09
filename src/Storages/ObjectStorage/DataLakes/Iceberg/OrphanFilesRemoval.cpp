#include "config.h"

#if USE_AVRO

#include <chrono>
#include <set>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/OrphanFilesRemoval.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace CurrentMetrics
{
extern const Metric MergeTreeBackgroundExecutorThreads;
extern const Metric MergeTreeBackgroundExecutorThreadsActive;
extern const Metric MergeTreeBackgroundExecutorThreadsScheduled;
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

namespace
{

enum class OrphanFileCategory : uint8_t
{
    DATA_FILE,
    POSITION_DELETE_FILE,
    EQUALITY_DELETE_FILE,
    MANIFEST_FILE,
    MANIFEST_LIST,
    METADATA_JSON,
    STATISTICS_FILE,
};

OrphanFileCategory categorizeFile(const String & relative_path)
{
    if (relative_path.find("/metadata/") != String::npos || relative_path.starts_with("metadata/"))
    {
        if (relative_path.ends_with(".metadata.json") || relative_path.ends_with(".metadata.json.gz"))
            return OrphanFileCategory::METADATA_JSON;
        if (relative_path.ends_with(".avro"))
        {
            if (relative_path.find("snap-") != String::npos)
                return OrphanFileCategory::MANIFEST_LIST;
            return OrphanFileCategory::MANIFEST_FILE;
        }
        if (relative_path.ends_with(".puffin") || relative_path.ends_with(".stats"))
            return OrphanFileCategory::STATISTICS_FILE;
    }

    if (relative_path.find("-deletes.parquet") != String::npos || relative_path.find("-delete-") != String::npos)
        return OrphanFileCategory::POSITION_DELETE_FILE;

    if (relative_path.find("-eq-del-") != String::npos)
        return OrphanFileCategory::EQUALITY_DELETE_FILE;

    return OrphanFileCategory::DATA_FILE;
}

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

    // 1e. Statistics files
    if (metadata->has(f_statistics))
    {
        auto statistics = metadata->get(f_statistics).extract<Poco::JSON::Array::Ptr>();
        if (statistics)
        {
            for (UInt32 i = 0; i < statistics->size(); ++i)
            {
                auto stat_entry = statistics->getObject(i);
                if (stat_entry->has("statistics-path"))
                {
                    String stat_path = stat_entry->getValue<String>("statistics-path");
                    String storage_path = getProperFilePathFromMetadataInfo(
                        stat_path, persistent_table_components.table_path, persistent_table_components.table_location);
                    reachable.insert(storage_path);
                }
            }
        }
    }

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

    // 1b–1d. For each snapshot: manifest list → manifests → data/delete files
    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        if (!snapshot->has(f_manifest_list))
            continue;

        String manifest_list_path = snapshot->getValue<String>(f_manifest_list);
        String storage_ml_path = getProperFilePathFromMetadataInfo(
            manifest_list_path, persistent_table_components.table_path, persistent_table_components.table_location);
        reachable.insert(storage_ml_path);

        ManifestFileCacheKeys manifest_keys;
        try
        {
            manifest_keys = getManifestList(
                object_storage, persistent_table_components, context, storage_ml_path, log);
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to read manifest list {}, skipping: {}", storage_ml_path, getCurrentExceptionMessage(false));
            continue;
        }

        for (const auto & mf_key : manifest_keys)
        {
            reachable.insert(mf_key.manifest_file_path);

            try
            {
                auto entries_handle = getManifestFileEntriesHandle(
                    object_storage, persistent_table_components, context, log,
                    mf_key, current_schema_id);

                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::DATA))
                    reachable.insert(entry->file_path);
                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                    reachable.insert(entry->file_path);
                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
                    reachable.insert(entry->file_path);
            }
            catch (...)
            {
                LOG_WARNING(log, "Failed to read manifest file {}, skipping: {}", mf_key.manifest_file_path, getCurrentExceptionMessage(false));
            }
        }
    }

    LOG_INFO(log, "Collected {} reachable files from metadata graph", reachable.size());
    return reachable;
}

void deleteOrphanFiles(
    const std::vector<String> & orphan_paths,
    ObjectStoragePtr object_storage,
    UInt64 max_concurrent_deletes,
    LoggerPtr log)
{
    if (max_concurrent_deletes == 0)
    {
        for (const auto & path : orphan_paths)
        {
            try
            {
                object_storage->removeObjectIfExists(StoredObject(path));
                LOG_DEBUG(log, "Deleted orphan file {}", path);
            }
            catch (...)
            {
                LOG_WARNING(log, "Failed to delete orphan file {}: {}", path, getCurrentExceptionMessage(false));
            }
        }
        return;
    }

    ThreadPool pool(
        CurrentMetrics::MergeTreeBackgroundExecutorThreads,
        CurrentMetrics::MergeTreeBackgroundExecutorThreadsActive,
        CurrentMetrics::MergeTreeBackgroundExecutorThreadsScheduled,
        max_concurrent_deletes);

    for (const auto & path : orphan_paths)
    {
        pool.scheduleOrThrowOnError([&object_storage, &path, &log]
        {
            try
            {
                object_storage->removeObjectIfExists(StoredObject(path));
                LOG_DEBUG(log, "Deleted orphan file {}", path);
            }
            catch (...)
            {
                LOG_WARNING(log, "Failed to delete orphan file {}: {}", path, getCurrentExceptionMessage(false));
            }
        });
    }
    pool.wait();
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
    RemoveOrphanFilesResult result;

    for (const auto & file_ptr : actual_files)
    {
        const String & path = file_ptr->relative_path;

        if (path.ends_with("version-hint.text"))
            continue;

        if (reachable.contains(path))
            continue;

        if (file_ptr->metadata.has_value())
        {
            auto file_modified = file_ptr->metadata->last_modified.epochTime();
            if (static_cast<time_t>(file_modified) >= older_than_threshold)
                continue;
        }

        auto category = categorizeFile(path);
        switch (category)
        {
            case OrphanFileCategory::DATA_FILE:
                ++result.deleted_data_files_count;
                break;
            case OrphanFileCategory::POSITION_DELETE_FILE:
                ++result.deleted_position_delete_files_count;
                break;
            case OrphanFileCategory::EQUALITY_DELETE_FILE:
                ++result.deleted_equality_delete_files_count;
                break;
            case OrphanFileCategory::MANIFEST_FILE:
                ++result.deleted_manifest_files_count;
                break;
            case OrphanFileCategory::MANIFEST_LIST:
                ++result.deleted_manifest_lists_count;
                break;
            case OrphanFileCategory::METADATA_JSON:
                ++result.deleted_metadata_files_count;
                break;
            case OrphanFileCategory::STATISTICS_FILE:
                ++result.deleted_statistics_files_count;
                break;
        }

        LOG_DEBUG(log, "Orphan file: {}", path);
        orphan_paths.push_back(path);
    }

    auto total = orphan_paths.size();
    LOG_INFO(log, "Found {} orphan files (dry_run={})", total, params.dry_run);

    // Step 4: Delete (unless dry_run)
    if (!params.dry_run && !orphan_paths.empty())
    {
        deleteOrphanFiles(orphan_paths, object_storage, params.max_concurrent_deletes, log);
        LOG_INFO(log, "Deleted {} orphan files", total);
    }

    return result;
}

}

#endif
