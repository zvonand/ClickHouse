#include "config.h"
#if USE_AVRO

#include <chrono>

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteCommandArgs.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/OrphanFilesRemoval.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/RemoveOrphanFilesExecute.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Setting
{
extern const SettingsUInt64 iceberg_orphan_files_older_than_seconds;
}

namespace Iceberg
{

namespace
{

ExecuteCommandArgs makeSchema()
{
    ExecuteCommandArgs schema("remove_orphan_files");
    schema.addPositional("older_than", Field::Types::String);
    schema.addNamed("location", Field::Types::String);
    schema.addNamed("dry_run", Field::Types::UInt64);
    schema.addDefault("dry_run", Field(UInt64(0)));
    return schema;
}

Pipe resultToPipe(const RemoveOrphanFilesResult & result)
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "metric_name"),
        ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), "metric_value"),
    };

    MutableColumns columns = header.cloneEmptyColumns();

    auto add = [&](const char * name, Int64 value)
    {
        columns[0]->insert(String(name));
        columns[1]->insert(value);
    };

    add("deleted_data_files_count", result.deleted_data_files_count);
    add("deleted_position_delete_files_count", result.deleted_position_delete_files_count);
    add("deleted_equality_delete_files_count", result.deleted_equality_delete_files_count);
    add("deleted_manifest_files_count", result.deleted_manifest_files_count);
    add("deleted_manifest_lists_count", result.deleted_manifest_lists_count);
    add("deleted_metadata_files_count", result.deleted_metadata_files_count);
    add("deleted_statistics_files_count", result.deleted_statistics_files_count);
    add("skipped_missing_metadata_count", result.skipped_missing_metadata_count);

    const size_t rows = columns[0]->size();
    Chunk chunk(std::move(columns), rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

} // anonymous namespace


Pipe executeRemoveOrphanFiles(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_components)
{
    auto parsed = makeSchema().parse(args);

    RemoveOrphanFilesParams params;
    if (parsed.has("older_than"))
    {
        ReadBufferFromString buf(parsed.getAs<String>("older_than"));
        time_t ts;
        readDateTimeText(ts, buf);

        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        if (ts > now)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "older_than must not be in the future; a future value would bypass the in-progress-write safety window");

        params.older_than = ts;
    }
    else
    {
        UInt64 threshold_seconds = context->getSettingsRef()[Setting::iceberg_orphan_files_older_than_seconds].value;
        auto now = std::chrono::system_clock::now();
        params.older_than = std::chrono::system_clock::to_time_t(now - std::chrono::seconds(threshold_seconds));
    }
    if (parsed.has("location"))
        params.location = parsed.getAs<String>("location");
    params.dry_run = parsed.getAs<UInt64>("dry_run") != 0;

    auto result = removeOrphanFiles(params, context, object_storage, data_lake_settings, persistent_components);

    return resultToPipe(result);
}

}
}

#endif
