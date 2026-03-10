#include "config.h"
#if USE_AVRO

#include <limits>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteOptionsParser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergCommandArgumentsParser.h>
#include <Storages/checkAndGetLiteralArgument.h>

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

std::vector<Int64> parseSnapshotIds(const Field & value_ast)
{
    std::vector<Int64> snapshot_ids;

    auto append_value = [&](const Field & value)
    {
        if (value.getType() == Field::Types::Int64)
        {
            snapshot_ids.push_back(value.safeGet<Int64>());
            return;
        }
        if (value.getType() == Field::Types::UInt64)
        {
            UInt64 id = value.safeGet<UInt64>();
            if (id > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots snapshot id is too large: {}", id);
            snapshot_ids.push_back(static_cast<Int64>(id));
            return;
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'snapshot_ids' to contain integer literals");
    };

    if (value_ast.getType() != Field::Types::Array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'snapshot_ids' to be an array literal like [1, 2, 3]");

    for (const auto & value : value_ast.safeGet<Array>())
        append_value(value);

    return snapshot_ids;
}

Int64 parseRetentionPeriodToMilliseconds(const Field & value_ast)
{
    if (value_ast.getType() != Field::Types::String)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "expire_snapshots expects 'retention_period' to be a string like '3d', '12h', '30m', '15s' or '250ms'");

    const String & input = value_ast.safeGet<String>();
    if (input.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots 'retention_period' cannot be empty");

    Decimal64 parsed_duration_ms;
    try
    {
        /// Scale=3 means the decimal stores milliseconds in the integer payload.
        parsed_duration_ms = parseTimeSeriesDuration(input, /* duration_scale */ 3);
    }
    catch (const Exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid retention_period '{}'", input);
    }

    Int64 milliseconds = parsed_duration_ms.value;
    if (milliseconds < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'retention_period' to be non-negative");

    return milliseconds;
}

}

ExpireSnapshotsOptions parseExpireSnapshotsOptions(const ASTPtr & args, ContextPtr context)
{
    ExpireSnapshotsOptions options;

    IcebergCommandArgumentsParser parser("expire_snapshots");

    parser.addPositional([&](const ASTPtr & node)
    {
        auto timestamp = tryGetLiteralArgument<String>(node, "timestamp");
        if (!timestamp)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots positional argument must be a single timestamp string");

        ReadBufferFromString buf(*timestamp);
        time_t expire_time;
        readDateTimeText(expire_time, buf);
        options.expire_before_ms = static_cast<Int64>(expire_time) * 1000;
    });

    parser.addNamedArg("retention_period", [&](const Field & value)
    {
        options.retention_period_ms = parseRetentionPeriodToMilliseconds(value);
    });

    parser.addNamedArg("retain_last", [&](const Field & value)
    {
        Int64 retain_last = parseInt64Field(value, "expire_snapshots", "retain_last");
        if (retain_last <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'retain_last' to be positive");
        if (retain_last > std::numeric_limits<Int32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots 'retain_last' is too large: {}", retain_last);
        options.retain_last = static_cast<Int32>(retain_last);
    });

    parser.addNamedArg("snapshot_ids", [&](const Field & value)
    {
        options.snapshot_ids = parseSnapshotIds(value);
    });

    parser.addNamedArg("dry_run", [&](const Field & value)
    {
        options.dry_run = parseBoolField(value, "expire_snapshots", "dry_run");
    });

    parser.addConstraint([&]()
    {
        if (options.snapshot_ids.has_value() && (options.retention_period_ms.has_value() || options.retain_last.has_value()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "expire_snapshots argument 'snapshot_ids' cannot be combined with 'retention_period' or 'retain_last'");
    });

    parser.parse(args, context);
    return options;
}

}
}

#endif
