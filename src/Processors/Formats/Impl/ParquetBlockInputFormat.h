#pragma once
#include "config.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Formats/FormatSettings.h>

namespace DB
{

struct ParquetFileBucketInfo : public FileBucketInfo
{
    std::vector<size_t> row_group_ids;

    ParquetFileBucketInfo() = default;
    explicit ParquetFileBucketInfo(const std::vector<size_t> & row_group_ids_);
    void serialize(WriteBuffer & buffer) override;
    void deserialize(ReadBuffer & buffer) override;
    String getIdentifier() const override;
    String getFormatName() const override
    {
        return "Parquet";
    }
};
using ParquetFileBucketInfoPtr = std::shared_ptr<ParquetFileBucketInfo>;

struct ParquetBucketSplitter : public IBucketSplitter
{
    ParquetBucketSplitter() = default;
    std::vector<FileBucketInfoPtr> splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_) override;
};

}

#endif
