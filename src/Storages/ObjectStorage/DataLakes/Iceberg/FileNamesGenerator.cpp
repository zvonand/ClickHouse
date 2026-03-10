#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>

#if USE_AVRO

namespace DB
{

FileNamesGenerator::FileNamesGenerator(
    bool use_uuid_in_metadata_,
    CompressionMethod compression_method_,
    const String & format_name_)
    : use_uuid_in_metadata(use_uuid_in_metadata_)
    , compression_method(compression_method_)
    , format_name(boost::to_lower_copy(format_name_))
{
}

FileNamesGenerator::FileNamesGenerator(const FileNamesGenerator & other)
{
    initial_version = other.initial_version;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;
}

FileNamesGenerator & FileNamesGenerator::operator=(const FileNamesGenerator & other)
{
    if (this == &other)
        return *this;

    initial_version = other.initial_version;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;

    return *this;
}

String FileNamesGenerator::generateDataFileName()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return fmt::format("data/data-{}.{}", uuid_str, format_name);
}

String FileNamesGenerator::generateManifestEntryName()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return fmt::format("metadata/{}.avro", uuid_str);
}

String FileNamesGenerator::generateManifestListName(Int64 snapshot_id, Int32 format_version)
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return fmt::format("metadata/snap-{}-{}-{}.avro", snapshot_id, format_version, uuid_str);
}

String FileNamesGenerator::generateMetadataName()
{
    auto compression_suffix = toContentEncodingName(compression_method);
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;
    if (!use_uuid_in_metadata)
    {
        auto res = fmt::format("metadata/v{}{}.metadata.json", initial_version, compression_suffix);
        initial_version++;
        return res;
    }
    else
    {
        auto uuid_str = uuid_generator.createRandom().toString();
        auto res = fmt::format("metadata/v{}-{}{}.metadata.json", initial_version, uuid_str, compression_suffix);
        initial_version++;
        return res;
    }
}

String FileNamesGenerator::generateVersionHint()
{
    return "metadata/version-hint.text";
}

String FileNamesGenerator::generatePositionDeleteFile()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return fmt::format("data/{}-deletes.{}", uuid_str, format_name);
}

}

#endif
