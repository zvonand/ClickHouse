#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

String IcebergPathResolver::resolve(const IcebergPathFromMetadata & metadata_path) const
{
    const auto & raw = metadata_path.getRawPath();

    /// The path from metadata is always a continuation of table_location.
    /// Strip the table_location prefix to get the relative suffix,
    /// then prepend table_root.
    if (raw.starts_with(table_location))
    {
        auto suffix = raw.substr(table_location.size());
        /// Strip leading '/' from suffix if present
        if (!suffix.empty() && suffix[0] == '/')
            suffix = suffix.substr(1);
        return table_root + suffix;
    }

    /// Fallback: table_location may be stored differently in metadata
    /// (e.g. with or without trailing slash, different URI scheme).
    /// Try to find the table_root path as a substring in the raw path.
    /// For example, raw = "wasb://container@host/iceberg_data/table/data/x.parquet"
    /// and table_root = "iceberg_data/table/" — we can find this in the raw path.
    if (!table_root.empty())
    {
        auto pos = raw.find(table_root);
        if (pos != String::npos)
            return String(raw.substr(pos));
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Cannot resolve Iceberg metadata path '{}': it does not start with table_location '{}' "
        "and does not contain table_root '{}'",
        raw,
        table_location,
        table_root);
}

}
