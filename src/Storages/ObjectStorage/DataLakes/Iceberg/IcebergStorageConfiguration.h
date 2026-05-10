#pragma once

#include <Core/Block_fwd.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFilterInfo.h>

#include <IO/CompressionMethod.h>
#include <Storages/prepareReadingFromFormat.h>

namespace DB
{

struct IcebergStorageConfiguration
{
    String storage_dir;
    String table_dir;
    String table_location;
    String format_name;
    SharedHeader shared_header;
    size_t max_block_size;
    FormatSettings format_settings;
    CompressionMethod compression_method;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;
    UInt64 max_rows_in_data_file = 0;
    UInt64 max_bytes_in_data_file = 0;
};

}
