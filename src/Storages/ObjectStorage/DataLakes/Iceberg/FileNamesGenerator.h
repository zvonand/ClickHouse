#pragma once

#include "config.h"

#include <IO/CompressionMethod.h>
#include <base/types.h>

#include <Poco/UUIDGenerator.h>

namespace DB
{

#if USE_AVRO

/// Generates relative file name suffixes for Iceberg table files.
/// The suffixes are relative to the table root, e.g. "data/data-uuid.parquet",
/// "metadata/snap-xxx.avro", "metadata/v3.metadata.json".
///
/// The caller is responsible for prepending the appropriate prefix
/// (via IcebergPathResolver) to get either storage paths or metadata paths.
class FileNamesGenerator
{
public:
    FileNamesGenerator() = default;
    explicit FileNamesGenerator(
        bool use_uuid_in_metadata_,
        CompressionMethod compression_method_,
        const String & format_name_);

    FileNamesGenerator(const FileNamesGenerator & other);
    FileNamesGenerator & operator=(const FileNamesGenerator & other);

    /// All generate* methods return a relative suffix like "data/xxx.parquet"
    /// or "metadata/snap-xxx.avro". Use IcebergPathResolver::storagePath()
    /// or IcebergPathResolver::metadataPath() to get full paths.
    String generateDataFileName();
    String generateManifestEntryName();
    String generateManifestListName(Int64 snapshot_id, Int32 format_version);
    String generateMetadataName();
    String generateVersionHint();
    String generatePositionDeleteFile();

    void setVersion(Int32 initial_version_) { initial_version = initial_version_; }
    void setCompressionMethod(CompressionMethod compression_method_) { compression_method = compression_method_; }

private:
    Poco::UUIDGenerator uuid_generator;
    bool use_uuid_in_metadata = false;
    CompressionMethod compression_method = CompressionMethod::None;
    String format_name;

    Int32 initial_version = 0;
};

#endif

}
