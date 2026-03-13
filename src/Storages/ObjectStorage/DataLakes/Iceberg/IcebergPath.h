#pragma once

#include <base/types.h>
#include <fmt/format.h>

namespace DB::Iceberg
{

/// Strong type for paths stored in Iceberg metadata files (Avro/JSON).
/// These paths may use various URI schemes (wasb://, s3://, abfss://, hdfs://, etc.)
/// or may be absolute paths (/table/data/xxx.parquet).
///
/// All paths written into Iceberg metadata MUST be of this type.
/// To get the actual storage path for I/O, pass through IcebergPathResolver::resolve().
///
/// This type is intentionally NOT implicitly convertible to String
/// to prevent accidental use of metadata paths as storage paths.
class IcebergPathFromMetadata
{
public:
    IcebergPathFromMetadata() = default;
    explicit IcebergPathFromMetadata(String path_) : raw_path(std::move(path_)) {}

    /// Access the raw path as stored in Iceberg metadata.
    /// Use only for: logging, comparison with other metadata paths,
    /// writing into Iceberg metadata files, or passing to IcebergPathResolver::resolve().
    const String & getRawPath() const { return raw_path; }

    bool empty() const { return raw_path.empty(); }

    bool operator==(const IcebergPathFromMetadata & other) const { return raw_path == other.raw_path; }
    bool operator<(const IcebergPathFromMetadata & other) const { return raw_path < other.raw_path; }
    bool operator<=(const IcebergPathFromMetadata & other) const { return raw_path <= other.raw_path; }
    bool operator>=(const IcebergPathFromMetadata & other) const { return raw_path >= other.raw_path; }

private:
    String raw_path;
};

/// Converts Iceberg metadata paths to actual object storage paths.
///
/// This is the ONLY way to go from a metadata path to a storage path.
/// The reverse direction is handled by FileNamesGenerator which always
/// produces metadata paths (IcebergPathFromMetadata).
///
/// The key invariant: path_from_metadata is always a continuation of table_location.
/// For example:
///   table_location = "wasb://container@account/iceberg_data/table"
///   path_from_metadata = "wasb://container@account/iceberg_data/table/data/xxx.parquet"
///   → relative suffix = "data/xxx.parquet"
///   → resolved storage path = "iceberg_data/table/data/xxx.parquet"
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

        /// Normalize: non-URI table_location should start with '/'
        /// (Iceberg spec expects absolute paths in metadata)
        if (!table_location.empty() && table_location.find("://") == String::npos && table_location[0] != '/')
            table_location = "/" + table_location;

        /// Normalize: ensure table_root ends with '/'
        if (!table_root.empty() && table_root.back() != '/')
            table_root += '/';
    }

    /// Convert a metadata path to an actual storage path for I/O operations.
    String resolve(const IcebergPathFromMetadata & metadata_path) const;

    /// Accessors for table_location (needed by FileNamesGenerator to construct metadata paths)
    /// and table_root (needed for logging / iceberg_metadata_log).
    const String & getTableLocation() const { return table_location; }
    const String & getTableRoot() const { return table_root; }

private:
    String table_location;
    String table_root;
};

}

/// Make IcebergPathFromMetadata hashable for use in unordered containers.
template <>
struct std::hash<DB::Iceberg::IcebergPathFromMetadata>
{
    size_t operator()(const DB::Iceberg::IcebergPathFromMetadata & p) const noexcept
    {
        return std::hash<String>{}(p.getRawPath());
    }
};

/// Make IcebergPathFromMetadata formattable with fmt for logging.
template <>
struct fmt::formatter<DB::Iceberg::IcebergPathFromMetadata> : fmt::formatter<std::string>
{
    auto format(const DB::Iceberg::IcebergPathFromMetadata & p, fmt::format_context & ctx) const
    {
        return fmt::formatter<std::string>::format(p.getRawPath(), ctx);
    }
};
