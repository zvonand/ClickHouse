#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>


namespace DB
{

struct PostgreSQLArrayInfo
{
    size_t num_dimensions;
    Field default_value;
    std::function<Field(std::string & field)> pqxx_parser;
};


void insertPostgreSQLValue(
        IColumn & column, std::string_view value,
        ExternalResultDescription::ValueType type, DataTypePtr data_type,
        const std::unordered_map<size_t, PostgreSQLArrayInfo> & array_info, size_t idx);

void preparePostgreSQLArrayInfo(
        std::unordered_map<size_t, PostgreSQLArrayInfo> & array_info, size_t column_idx, DataTypePtr data_type);

void insertDefaultPostgreSQLValue(IColumn & column, const IColumn & sample_column);

}

#endif
