#pragma once

#include "config.h"

#if USE_AVRO

#include <unordered_set>

#include <Common/Logger_fwd.h>
#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Array.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

struct SnapshotReferencedFiles
{
    std::unordered_set<String> manifest_list_metadata_paths;
    std::unordered_set<String> manifest_list_storage_paths;
    std::unordered_set<String> manifest_paths;
    std::unordered_set<String> data_file_paths;
};

SnapshotReferencedFiles collectSnapshotReferencedFiles(
    const Poco::JSON::Array::Ptr & snapshots,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id);

/// Collect all files reachable through the metadata graph.
///
/// Traverses: metadata JSON files (from metadata-log), manifest lists (from snapshots),
/// manifest files (from manifest lists), data/delete files (from manifest files),
/// statistics files, and the version-hint file.
std::unordered_set<String> collectReachableFiles(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log);

}

#endif
