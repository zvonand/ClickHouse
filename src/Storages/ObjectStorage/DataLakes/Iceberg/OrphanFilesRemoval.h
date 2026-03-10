#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

struct RemoveOrphanFilesResult
{
    Int64 deleted_data_files_count = 0;
    Int64 deleted_position_delete_files_count = 0;
    Int64 deleted_equality_delete_files_count = 0;
    Int64 deleted_manifest_files_count = 0;
    Int64 deleted_manifest_lists_count = 0;
    Int64 deleted_metadata_files_count = 0;
    Int64 deleted_statistics_files_count = 0;
    Int64 skipped_missing_metadata_count = 0;
};

struct RemoveOrphanFilesParams
{
    std::optional<time_t> older_than;
    std::optional<String> location;
    bool dry_run = false;
    UInt64 max_concurrent_deletes = 0;
};

RemoveOrphanFilesResult removeOrphanFiles(
    const RemoveOrphanFilesParams & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_table_components);

}

#endif
