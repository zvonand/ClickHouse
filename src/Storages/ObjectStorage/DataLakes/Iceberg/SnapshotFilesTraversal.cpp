#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>

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

}

#endif
