#include "config.h"

#if USE_AVRO

#include <unordered_set>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/OrphanFilesRemoval.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

struct OrphanScanResult
{
    std::vector<String> orphan_paths;
    Int64 skipped_missing_metadata = 0;
};

namespace
{

String resolveScanPath(const String & table_path, const RemoveOrphanFilesParams & params)
{
    String scan_path = table_path;
    if (params.location.has_value())
    {
        String loc = *params.location;
        if (loc.find("..") != String::npos || loc.starts_with('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "location must be a relative path under the table root, got '{}'", loc);

        while (loc.starts_with("./"))
            loc = loc.substr(2);

        if (!scan_path.ends_with('/'))
            scan_path += '/';
        scan_path += loc;

        if (!scan_path.ends_with('/'))
            scan_path += '/';
    }
    return scan_path;
}

OrphanScanResult findOrphanFiles(
    const RelativePathsWithMetadata & actual_files,
    const std::unordered_set<String> & reachable,
    time_t older_than_threshold,
    LoggerPtr log)
{
    OrphanScanResult scan;

    for (const auto & file_ptr : actual_files)
    {
        const String & path = file_ptr->relative_path;

        if (path.ends_with("version-hint.text"))
            continue;

        if (reachable.contains(path))
            continue;

        if (!file_ptr->metadata.has_value())
        {
            ++scan.skipped_missing_metadata;
            LOG_DEBUG(log, "Skipping file without metadata (no last_modified): {}", path);
            continue;
        }

        auto file_modified = file_ptr->metadata->last_modified.epochTime();
        if (static_cast<time_t>(file_modified) >= older_than_threshold)
            continue;

        LOG_DEBUG(log, "Orphan file: {}", path);
        scan.orphan_paths.push_back(path);
    }

    if (scan.skipped_missing_metadata > 0)
        LOG_WARNING(log, "Skipped {} unreferenced file(s) because last_modified metadata was unavailable; "
            "these files could not be age-checked and were conservatively kept", scan.skipped_missing_metadata);

    return scan;
}

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

RemoveOrphanFilesResult tallyByCategory(const std::vector<String> & paths, Int64 skipped_missing_metadata)
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

    auto reachable = collectReachableFiles(
        object_storage, persistent_table_components, data_lake_settings, context, log);

    String scan_path = resolveScanPath(persistent_table_components.table_path, params);
    RelativePathsWithMetadata actual_files;
    object_storage->listObjects(scan_path, actual_files, /* max_keys = */ 0);
    LOG_INFO(log, "Found {} actual files under scan path '{}'", actual_files.size(), scan_path);

    chassert(params.older_than.has_value());
    auto scan = findOrphanFiles(actual_files, reachable, *params.older_than, log);
    LOG_INFO(log, "Found {} orphan files (dry_run={})", scan.orphan_paths.size(), params.dry_run);

    if (params.dry_run || scan.orphan_paths.empty())
        return tallyByCategory(scan.orphan_paths, scan.skipped_missing_metadata);

    auto deleted = deleteOrphanFiles(scan.orphan_paths, object_storage, log);
    LOG_INFO(log, "Deleted {}/{} orphan files", deleted.size(), scan.orphan_paths.size());

    return tallyByCategory(deleted, scan.skipped_missing_metadata);
}

}

#endif
