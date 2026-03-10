#pragma once

#include "config.h"

#if USE_AVRO

#include <set>

#include <Common/Logger_fwd.h>
#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Array.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

struct SnapshotReferencedFiles
{
    std::set<String> manifest_list_metadata_paths;
    std::set<String> manifest_list_storage_paths;
    std::set<String> manifest_paths;
    std::set<String> data_file_paths;
};

SnapshotReferencedFiles collectSnapshotReferencedFiles(
    const Poco::JSON::Array::Ptr & snapshots,
    ObjectStoragePtr object_storage,
    PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id);

}

#endif
