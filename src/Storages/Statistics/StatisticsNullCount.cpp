#include <Storages/Statistics/StatisticsNullCount.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StatisticsNullCount::StatisticsNullCount(const SingleStatisticsDescription & description)
    : IStatistics(description)
{
}

void StatisticsNullCount::build(const ColumnPtr & column)
{
    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(column.get()))
    {
        const auto & null_map = nullable_col->getNullMapData();
        null_count += std::count(null_map.begin(), null_map.end(), 1);
        return;
    }

    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        if (!lc_col->nestedIsNullable())
            return;

        size_t null_index = lc_col->getDictionary().getNullValueIndex();
        const auto & indexes = lc_col->getIndexes();

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes.getUInt(i) == null_index)
                ++null_count;
        return;
    }

    chassert(false && "Unexpected column type in NullCount::build");
}

void StatisticsNullCount::merge(const StatisticsPtr & other_stats)
{
    const auto * other = typeid_cast<const StatisticsNullCount *>(other_stats.get());
    if (!other)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge NullCount statistics with a different type");
    null_count += other->null_count;
}

void StatisticsNullCount::serialize(WriteBuffer & buf)
{
    writeIntBinary(null_count, buf);
}

void StatisticsNullCount::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    readIntBinary(null_count, buf);
}

String StatisticsNullCount::getNameForLogs() const
{
    return fmt::format("NullCount: {}", null_count);
}

bool nullCountStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    return isNullableOrLowCardinalityNullable(data_type);
}

StatisticsPtr nullCountStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & /*data_type*/)
{
    return std::make_shared<StatisticsNullCount>(description);
}

}
