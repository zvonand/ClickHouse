#include "config.h"
#if USE_AVRO

#include <limits>

#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteCommandArgs.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExpireSnapshotsExecute.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExpireSnapshotsTypes.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Iceberg
{

namespace
{

ExecuteCommandArgs makeSchema()
{
    ExecuteCommandArgs schema("expire_snapshots");
    schema.addPositional("expire_before", Field::Types::String);
    schema.addNamed("retention_period");
    schema.addNamed("retain_last");
    schema.addNamed("snapshot_ids");
    schema.addNamed("dry_run");
    schema.addDefault("dry_run", Field(UInt64(0)));
    return schema;
}

ExpireSnapshotsOptions buildOptions(const ExecuteCommandArgs::Result & parsed)
{
    ExpireSnapshotsOptions options;
    static constexpr std::string_view cmd = "expire_snapshots";

    if (parsed.has("expire_before"))
    {
        String ts = parsed.getAs<String>("expire_before");
        ReadBufferFromString buf(ts);
        time_t expire_time;
        readDateTimeText(expire_time, buf);
        options.expire_before_ms = static_cast<Int64>(expire_time) * 1000;
    }

    if (parsed.has("retention_period"))
        options.retention_period_ms = fieldToPeriodMs(parsed.get("retention_period"), cmd, "retention_period");

    if (parsed.has("retain_last"))
    {
        Int64 retain_last = fieldToInt64(parsed.get("retain_last"), cmd, "retain_last");
        if (retain_last <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'retain_last' to be positive");
        if (retain_last > std::numeric_limits<Int32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots 'retain_last' is too large: {}", retain_last);
        options.retain_last = static_cast<Int32>(retain_last);
    }

    if (parsed.has("snapshot_ids"))
        options.snapshot_ids = fieldToInt64Array(parsed.get("snapshot_ids"), cmd, "snapshot_ids");

    if (parsed.has("dry_run"))
        options.dry_run = fieldToBool(parsed.get("dry_run"), cmd, "dry_run");

    if (options.snapshot_ids && (options.retention_period_ms || options.retain_last))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "expire_snapshots argument 'snapshot_ids' cannot be combined with 'retention_period' or 'retain_last'");

    return options;
}

Pipe resultToPipe(const ExpireSnapshotsResult & result)
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
    add("deleted_statistics_files_count", result.deleted_statistics_files_count);
    add("dry_run", result.dry_run ? 1 : 0);

    const size_t rows = columns[0]->size();
    Chunk chunk(std::move(columns), rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

} // anonymous namespace


Pipe executeExpireSnapshots(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & blob_storage_type_name,
    const String & blob_storage_namespace_name,
    const String & table_name)
{
    auto parsed = makeSchema().parse(args);
    auto options = buildOptions(parsed);

    auto result = expireSnapshots(
        options,
        context,
        object_storage,
        data_lake_settings,
        persistent_components,
        write_format,
        catalog,
        blob_storage_type_name,
        blob_storage_namespace_name,
        table_name);

    return resultToPipe(result);
}

} // namespace Iceberg
} // namespace DB

#endif
