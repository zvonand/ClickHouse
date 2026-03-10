#include "config.h"
#if USE_AVRO

#include <limits>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteOptionsParser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergCommandArgumentsParser.h>

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

std::vector<Int64> parseSnapshotIds(const Array & arr)
{
    std::vector<Int64> snapshot_ids;
    for (const auto & elem : arr)
    {
        if (elem.getType() == Field::Types::Int64)
        {
            snapshot_ids.push_back(elem.safeGet<Int64>());
        }
        else if (elem.getType() == Field::Types::UInt64)
        {
            UInt64 id = elem.safeGet<UInt64>();
            if (id > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots snapshot id is too large: {}", id);
            snapshot_ids.push_back(static_cast<Int64>(id));
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'snapshot_ids' to contain integer literals");
        }
    }
    return snapshot_ids;
}

Int64 parseRetentionPeriodToMilliseconds(const String & input)
{
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
    IcebergCommandArgumentsParser parser("expire_snapshots");

    parser.addPositional();
    parser.addNamedArg("retention_period", ArgType::String);
    parser.addNamedArg("retain_last", ArgType::Int64);
    parser.addNamedArg("snapshot_ids", ArgType::Array);
    parser.addNamedArg("dry_run", ArgType::Bool);

    parser.addConstraint([](const ParsedArguments & parsed)
    {
        if (parsed.has("snapshot_ids") && (parsed.has("retention_period") || parsed.has("retain_last")))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "expire_snapshots argument 'snapshot_ids' cannot be combined with 'retention_period' or 'retain_last'");
    });

    auto parsed = parser.parse(args, context);

    ExpireSnapshotsOptions options;

    if (!parsed.positional().empty())
    {
        String timestamp = parsed.positional()[0].safeGet<String>();
        ReadBufferFromString buf(timestamp);
        time_t expire_time;
        readDateTimeText(expire_time, buf);
        options.expire_before_ms = static_cast<Int64>(expire_time) * 1000;
    }

    if (parsed.has("retention_period"))
        options.retention_period_ms = parseRetentionPeriodToMilliseconds(parsed.getString("retention_period"));

    if (parsed.has("retain_last"))
    {
        Int64 retain_last = parsed.getInt64("retain_last");
        if (retain_last <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'retain_last' to be positive");
        if (retain_last > std::numeric_limits<Int32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots 'retain_last' is too large: {}", retain_last);
        options.retain_last = static_cast<Int32>(retain_last);
    }

    if (parsed.has("snapshot_ids"))
        options.snapshot_ids = parseSnapshotIds(parsed.getArray("snapshot_ids"));

    if (parsed.has("dry_run"))
        options.dry_run = parsed.getBool("dry_run");

    return options;
}

}
}

#endif
