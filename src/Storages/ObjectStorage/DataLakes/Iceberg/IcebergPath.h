#pragma once

#include <base/types.h>
#include <fmt/format.h>

namespace DB::Iceberg
{

/// Strong type for paths read from Iceberg metadata files (Avro/JSON).
/// These paths may use various URI schemes (wasb://, s3://, abfss://, hdfs://, etc.)
/// or may be relative paths (/table/data/xxx.parquet).
///
/// Paths from metadata MUST be resolved through `IcebergPathResolver` before
/// use in storage operations. This type prevents accidental use of raw metadata
/// paths as storage paths by not being implicitly convertible to String.
class IcebergPathFromMetadata
{
public:
    IcebergPathFromMetadata() = default;
    explicit IcebergPathFromMetadata(String path_) : raw_path(std::move(path_)) {}

    /// Access the raw path as stored in Iceberg metadata.
    /// Use only for logging, comparison with other metadata paths,
    /// or passing to IcebergPathResolver.
    const String & getRawPath() const { return raw_path; }

    bool empty() const { return raw_path.empty(); }

    bool operator==(const IcebergPathFromMetadata & other) const { return raw_path == other.raw_path; }
    bool operator<(const IcebergPathFromMetadata & other) const { return raw_path < other.raw_path; }
    bool operator<=(const IcebergPathFromMetadata & other) const { return raw_path <= other.raw_path; }
    bool operator>=(const IcebergPathFromMetadata & other) const { return raw_path >= other.raw_path; }

private:
    String raw_path;
};

/// Resolves paths read from Iceberg metadata to actual object storage paths.
///
/// Iceberg metadata may store paths in different formats:
///   - Full URIs: wasb://container@account/iceberg_data/table/data/xxx.parquet
///   - Relative paths: /iceberg_data/table/data/xxx.parquet
///
/// The key invariant: path_from_metadata is always a continuation of table_location.
/// For example:
///   table_location = "wasb://container@account/iceberg_data/table"
///   path_from_metadata = "wasb://container@account/iceberg_data/table/data/xxx.parquet"
///   → relative suffix = "data/xxx.parquet"
///   → resolved storage path = "iceberg_data/table/data/xxx.parquet" (using table_root)
class IcebergPathResolver
{
public:
    IcebergPathResolver(String table_location_, String table_root_)
        : table_location(std::move(table_location_))
        , table_root(std::move(table_root_))
    {
        /// Normalize: ensure table_location does not end with '/'
        while (!table_location.empty() && table_location.back() == '/')
            table_location.pop_back();

        /// Normalize: ensure table_root ends with '/'
        if (!table_root.empty() && table_root.back() != '/')
            table_root += '/';
    }

    /// Convert a metadata path to an actual storage path.
    String resolve(const IcebergPathFromMetadata & metadata_path) const;

    const String & getTableRoot() const { return table_root; }

private:
    String table_location;
    String table_root;
};

}

/// Make IcebergPathFromMetadata formattable with fmt for logging.
template <>
struct fmt::formatter<DB::Iceberg::IcebergPathFromMetadata> : fmt::formatter<std::string>
{
    auto format(const DB::Iceberg::IcebergPathFromMetadata & p, fmt::format_context & ctx) const
    {
        return fmt::formatter<std::string>::format(p.getRawPath(), ctx);
    }
};
