#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>

#if USE_AVRO

namespace DB
{

FileNamesGenerator::FileNamesGenerator(
    const String & table_location_,
    bool use_uuid_in_metadata_,
    CompressionMethod compression_method_,
    const String & format_name_)
    : table_location(table_location_)
    , use_uuid_in_metadata(use_uuid_in_metadata_)
    , compression_method(compression_method_)
    , format_name(boost::to_lower_copy(format_name_))
{
    /// Normalize: ensure table_location ends with '/'
    if (!table_location.empty() && table_location.back() != '/')
        table_location += '/';
}

FileNamesGenerator::FileNamesGenerator(const FileNamesGenerator & other)
{
    initial_version = other.initial_version;
    table_location = other.table_location;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;
}

FileNamesGenerator & FileNamesGenerator::operator=(const FileNamesGenerator & other)
{
    if (this == &other)
        return *this;

    initial_version = other.initial_version;
    table_location = other.table_location;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;

    return *this;
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateDataFileName()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}data/data-{}.{}", table_location, uuid_str, format_name));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateManifestEntryName()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/{}.avro", table_location, uuid_str));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateManifestListName(Int64 snapshot_id, Int32 format_version)
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/snap-{}-{}-{}.avro", table_location, snapshot_id, format_version, uuid_str));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateMetadataName()
{
    auto compression_suffix = toContentEncodingName(compression_method);
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;
    if (!use_uuid_in_metadata)
    {
        auto res = Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/v{}{}.metadata.json", table_location, initial_version, compression_suffix));
        initial_version++;
        return res;
    }
    else
    {
        auto uuid_str = uuid_generator.createRandom().toString();
        auto res = Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/v{}-{}{}.metadata.json", table_location, initial_version, uuid_str, compression_suffix));
        initial_version++;
        return res;
    }
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateVersionHint()
{
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/version-hint.text", table_location));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generatePositionDeleteFile()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}data/{}-deletes.{}", table_location, uuid_str, format_name));
}

}

#endif
