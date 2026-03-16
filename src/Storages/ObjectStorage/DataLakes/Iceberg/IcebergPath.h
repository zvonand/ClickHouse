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
    IcebergPathResolver(String table_location_, String table_root_, String blob_storage_type_name_ = {}, String blob_storage_namespace_name_ = {})
        : table_location(std::move(table_location_))
        , table_root(std::move(table_root_))
        , blob_storage_type_name(std::move(blob_storage_type_name_))
        , blob_storage_namespace_name(std::move(blob_storage_namespace_name_))
    {
        auto trim_backward_slashes = [](String & str)
        {
            while (!str.empty() && str.back() == '/')
                str.pop_back();
        };
        trim_backward_slashes(table_root);
        trim_backward_slashes(table_location);

        /// Normalize: non-URI table_location should start with '/'
        /// (Iceberg spec expects absolute paths in metadata)
        if (!table_location.empty() && table_location.find("://") == String::npos && table_location[0] != '/')
            table_location = "/" + table_location;
    }

    /// Convert a metadata path to an actual storage path for I/O operations.
    String resolve(const IcebergPathFromMetadata & metadata_path) const;

    /// Convert a metadata path to a catalog-compatible path.
    /// Ensures the path starts with the storage type scheme (e.g. s3://, azure://).
    String resolveForCatalog(const IcebergPathFromMetadata & metadata_path) const
    {
        String catalog_filename = metadata_path.getRawPath();
        if (!catalog_filename.starts_with(blob_storage_type_name))
            catalog_filename = blob_storage_type_name + "://" + blob_storage_namespace_name + "/" + catalog_filename;
        return catalog_filename;
    }

    /// Accessors for table_location (needed by FileNamesGenerator to construct metadata paths)
    /// and table_root (needed for logging / iceberg_metadata_log).
    const String & getTableLocation() const { return table_location; }
    const String & getTableRoot() const { return table_root; }

private:
    String table_location;
    String table_root;
    String blob_storage_type_name;
    String blob_storage_namespace_name;
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
