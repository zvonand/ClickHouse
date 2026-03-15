#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>

#include <Poco/JSON/Object.h>

#include <Common/logger_useful.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace DB::Iceberg
{

SnapshotReferencedFiles collectSnapshotReferencedFiles(
    const Poco::JSON::Array::Ptr & snapshots,
    ObjectStoragePtr object_storage,
    PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id)
{
    SnapshotReferencedFiles files;

    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        if (!snapshot->has(Iceberg::f_manifest_list))
            continue;

        String manifest_list_path = snapshot->getValue<String>(Iceberg::f_manifest_list);
        files.manifest_list_metadata_paths.insert(manifest_list_path);

        String storage_manifest_list_path = getProperFilePathFromMetadataInfo(
            manifest_list_path, persistent_table_components.table_path, persistent_table_components.table_location);
        files.manifest_list_storage_paths.insert(storage_manifest_list_path);

        auto manifest_keys = getManifestList(
            object_storage, persistent_table_components, context, storage_manifest_list_path, log);

        for (const auto & mf_key : manifest_keys)
        {
            files.manifest_paths.insert(mf_key.manifest_file_path);

            auto entries_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log, mf_key, current_schema_id);

            for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::DATA))
                files.data_file_paths.insert(entry->file_path);
            for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                files.data_file_paths.insert(entry->file_path);
            for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
                files.data_file_paths.insert(entry->file_path);
        }
    }

    return files;
}

namespace
{

void collectStatisticsPaths(
    const Poco::JSON::Object::Ptr & metadata,
    const char * field_name,
    const String & table_path,
    const String & table_location,
    std::set<String> & out)
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
            out.insert(getProperFilePathFromMetadataInfo(stat_path, table_path, table_location));
        }
    }
}

/// Collect files reachable directly from the metadata JSON root:
/// the current metadata file, historical metadata files from metadata-log,
/// statistics and partition-statistics files, and the version-hint file.
void collectMetadataRootFiles(
    const String & metadata_path,
    const Poco::JSON::Object::Ptr & metadata,
    const String & table_path,
    const String & table_location,
    std::set<String> & out)
{
    out.insert(metadata_path);

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
                    out.insert(getProperFilePathFromMetadataInfo(mf_path, table_path, table_location));
                }
            }
        }
    }

    collectStatisticsPaths(metadata, f_statistics, table_path, table_location, out);
    collectStatisticsPaths(metadata, f_partition_statistics, table_path, table_location, out);

    String version_hint = table_path;
    if (!version_hint.ends_with('/'))
        version_hint += '/';
    version_hint += "metadata/version-hint.text";
    out.insert(version_hint);
}

} // anonymous namespace


std::set<String> collectReachableFiles(
    ObjectStoragePtr object_storage,
    PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log)
{
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

    std::set<String> reachable;

    collectMetadataRootFiles(
        metadata_path, metadata,
        persistent_table_components.table_path,
        persistent_table_components.table_location,
        reachable);

    if (!metadata->has(f_snapshots))
    {
        LOG_INFO(log, "No snapshots in metadata, reachable set contains only metadata-root files");
        return reachable;
    }

    auto snapshots = metadata->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    if (!snapshots || snapshots->size() == 0)
    {
        LOG_INFO(log, "Empty snapshots array, reachable set contains only metadata-root files");
        return reachable;
    }

    Int32 current_schema_id = metadata->getValue<Int32>(f_current_schema_id);

    auto snapshot_files = collectSnapshotReferencedFiles(
        snapshots, object_storage, persistent_table_components, context, log, current_schema_id);
    reachable.insert(snapshot_files.manifest_list_storage_paths.begin(), snapshot_files.manifest_list_storage_paths.end());
    reachable.insert(snapshot_files.manifest_paths.begin(), snapshot_files.manifest_paths.end());
    reachable.insert(snapshot_files.data_file_paths.begin(), snapshot_files.data_file_paths.end());

    LOG_INFO(log, "Collected {} reachable files from metadata graph", reachable.size());
    return reachable;
}

}

#endif
