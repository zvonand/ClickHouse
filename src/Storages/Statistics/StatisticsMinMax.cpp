#include <Storages/Statistics/StatisticsMinMax.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(data_type_))
{
}

void StatisticsMinMax::build(const ColumnPtr & column)
{
    Field min_field;
    Field max_field;

    column->getExtremes(min_field, max_field, 0, column->size());

    if (!min_field.isNull())
    {
        if (min.isNull() || min_field < min)
            min = min_field;
    }

    if (!max_field.isNull())
    {
        if (max.isNull() || max_field > max)
            max = max_field;
    }

    row_count += column->size();
}

void StatisticsMinMax::merge(const StatisticsPtr & other_stats)
{
    const StatisticsMinMax * other = typeid_cast<const StatisticsMinMax *>(other_stats.get());
    if (!other->min.isNull() && (min.isNull() || other->min < min))
        min = other->min;
    if (!other->max.isNull() && (max.isNull() || other->max > max))
        max = other->max;
}

void StatisticsMinMax::serialize(WriteBuffer & buf)
{
    writeIntBinary(row_count, buf);
    writeStringBinary(data_type->getName(), buf);
    writeFieldBinary(min, buf);
    writeFieldBinary(max, buf);
}

void StatisticsMinMax::deserialize(ReadBuffer & buf)
{
    readIntBinary(row_count, buf);

    /// Type name followed by Field-typed min and max
    String stored_type_name;
    readStringBinary(stored_type_name, buf);
    min = readFieldBinary(buf);
    max = readFieldBinary(buf);

    if (stored_type_name != data_type->getName())
    {
        /// Column type has changed — try to convert min/max to the new type
        auto stored_type = DataTypeFactory::instance().get(stored_type_name);
        if (!min.isNull())
        {
            Field converted = convertFieldToType(min, *data_type, stored_type.get());
            min = std::move(converted); /// null on conversion failure → effectively resets the bound
        }
        if (!max.isNull())
        {
            Field converted = convertFieldToType(max, *data_type, stored_type.get());
            max = std::move(converted);
        }
    }
}

std::optional<Float64> StatisticsMinMax::estimateLess(const Field & val) const
{
    if (row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    try
    {
        auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
        auto min_as_float = StatisticsUtils::tryConvertToFloat64(min, data_type);
        auto max_as_float = StatisticsUtils::tryConvertToFloat64(max, data_type);
        if (!val_as_float || !min_as_float || !max_as_float)
            return std::nullopt;

        if (val_as_float < min_as_float)
            return 0.0;
        if (val_as_float > max_as_float)
            return static_cast<Float64>(row_count);
        if (min_as_float == max_as_float)
            return (val_as_float != max_as_float) ? 0.0 : static_cast<Float64>(row_count);
        return ((*val_as_float - *min_as_float) / (*max_as_float - *min_as_float)) * static_cast<Float64>(row_count);
    }
    catch (...)
    {
        tryLogCurrentException("StatisticsMinMax", "While estimating less-than selectivity", LogsLevel::warning);
        return std::nullopt;
    }
}

String StatisticsMinMax::getNameForLogs() const
{
    return fmt::format("MinMax: ({}, {})", applyVisitor(FieldVisitorToString(), min), applyVisitor(FieldVisitorToString(), max));
}

bool minMaxStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    auto inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber();
}

StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsMinMax>(description, data_type);
}

}
