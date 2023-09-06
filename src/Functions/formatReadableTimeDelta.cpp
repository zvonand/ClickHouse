#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Prints amount of seconds in form of:
  * "1 year, 2 months, 12 days, 3 hours, 1 minute and 33 seconds".
  * Maximum unit can be specified as a second argument: for example, you can specify "days",
  * and it will avoid using years and months.
  *
  * The length of years and months (and even days in presence of time adjustments) are rough:
  * year is just 365 days, month is 30.5 days, day is 86400 seconds.
  *
  * You may think that the choice of constants and the whole purpose of this function is very ignorant...
  * And you're right. But actually it's made similar to a random Python library from the internet:
  * https://github.com/jmoiron/humanize/blob/b37dc30ba61c2446eecb1a9d3e9ac8c9adf00f03/src/humanize/time.py#L462
  */
class FunctionFormatReadableTimeDelta : public IFunction
{
public:
    static constexpr auto name = "formatReadableTimeDelta";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatReadableTimeDelta>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 1.",
                getName(), arguments.size());

        if (arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1, 2 or 3.",
                getName(), arguments.size());

        const IDataType & type = *arguments[0];

        if (!isNativeNumber(type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} as time delta", type.getName());

        if (arguments.size() >= 2)
        {
            const auto * maximum_unit_arg = arguments[1].get();
            if (!isStringOrFixedString(maximum_unit_arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument maximum_unit of function {}",
                                maximum_unit_arg->getName(), getName());

            if (arguments.size() == 3)
            {
                const auto * minimum_unit_arg = arguments[2].get();
                if (!isStringOrFixedString(minimum_unit_arg))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument minimum_unit of function {}",
                                    minimum_unit_arg->getName(), getName());
            }
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool useDefaultImplementationForConstants() const override { return true; }

    enum Unit
    {
        Nanoseconds = 1,
        Microseconds = 2,
        Milliseconds = 3,
        Seconds = 4,
        Minutes = 5,
        Hours = 6,
        Days = 7,
        Months = 8,
        Years = 9
    };

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::string_view maximum_unit_str, minimum_unit_str;
        if (arguments.size() >= 2)
        {
            const ColumnPtr & maximum_unit_column = arguments[1].column;
            const ColumnConst * maximum_unit_const_col = checkAndGetColumnConstStringOrFixedString(maximum_unit_column.get());
            if (maximum_unit_const_col)
                maximum_unit_str = maximum_unit_const_col->getDataColumn().getDataAt(0).toView();

            if (arguments.size() == 3)
            {
                const ColumnPtr & minimum_unit_column = arguments[2].column;
                const ColumnConst * minimum_unit_const_col = checkAndGetColumnConstStringOrFixedString(minimum_unit_column.get());
                if (minimum_unit_const_col)
                    minimum_unit_str = minimum_unit_const_col->getDataColumn().getDataAt(0).toView();
            }
        }
        Unit max_unit, min_unit;

        /// Default means "use all available whole units".
        if (maximum_unit_str.empty() || maximum_unit_str == "years")
            max_unit = Years;
        else if (maximum_unit_str == "months")
            max_unit = Months;
        else if (maximum_unit_str == "days")
            max_unit = Days;
        else if (maximum_unit_str == "hours")
            max_unit = Hours;
        else if (maximum_unit_str == "minutes")
            max_unit = Minutes;
        else if (maximum_unit_str == "seconds")
            max_unit = Seconds;
        else if (maximum_unit_str == "milliseconds")
            max_unit = Milliseconds;
        else if (maximum_unit_str == "microseconds")
            max_unit = Microseconds;
        else if (maximum_unit_str == "nanoseconds")
            max_unit = Nanoseconds;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected value of maximum unit argument ({}) for function {}, the only allowed values are:"
                " 'nanoseconds', 'microseconds', 'nanoseconds', 'seconds', 'minutes', 'hours', 'days', 'months', 'years'.",
                maximum_unit_str, getName());

        if (minimum_unit_str.empty() || minimum_unit_str == "seconds") // Set seconds as min_unit by default not to ruin old use cases
            min_unit = Seconds;
        else if (minimum_unit_str == "years")
            min_unit = Years;
        else if (minimum_unit_str == "months")
            min_unit = Months;
        else if (minimum_unit_str == "days")
            min_unit = Days;
        else if (minimum_unit_str == "hours")
            min_unit = Hours;
        else if (minimum_unit_str == "minutes")
            min_unit = Minutes;
        else if (minimum_unit_str == "milliseconds")
            min_unit = Milliseconds;
        else if (minimum_unit_str == "microseconds")
            min_unit = Microseconds;
        else if (minimum_unit_str == "nanoseconds")
            min_unit = Nanoseconds;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Unexpected value of minimum unit argument ({}) for function {}, the only allowed values are:"
                            " 'nanoseconds', 'microseconds', 'nanoseconds', 'seconds', 'minutes', 'hours', 'days', 'months', 'years'.",
                            minimum_unit_str, getName());

        if (min_unit > max_unit)
        {
            if (minimum_unit_str.empty())
                min_unit = Nanoseconds;   /// User wants sub-second max_unit. Show him all sub-second units unless other specified.
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Explicitly specified value of minimum unit argument ({}) for function {} "
                                "must not be greater than maximum unit value ({}).",
                                minimum_unit_str, getName(), maximum_unit_str);
        }

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// Virtual call is Ok (negligible comparing to the rest of calculations).
            Float64 value = arguments[0].column->getFloat64(i);

            if (!isFinite(value))
            {
                /// Cannot decide what unit it is (years, month), just simply write inf or nan.
                writeFloatText(value, buf_to);
            }
            else
            {
                bool is_negative = value < 0;
                if (is_negative)
                {
                    writeChar('-', buf_to);
                    value = -value;
                }

                /// To output separators between parts: ", " and " and ".
                bool has_output = false;

                switch (max_unit) /// A kind of Duff Device.
                {
                    case Years:
                        processUnit(365 * 24 * 3600, 1, " year", 5, value, buf_to, has_output, min_unit, min_unit == Years);
                        if (min_unit == Years)
                            break;
                        [[fallthrough]];

                    case Months:
                        processUnit(static_cast<UInt64>(30.5 * 24 * 3600), 1, " month", 6, value, buf_to, has_output, min_unit, min_unit == Months);
                        if (min_unit == Months)
                            break;
                        [[fallthrough]];

                    case Days:
                        processUnit(24 * 3600, 1, " day", 4, value, buf_to, has_output, min_unit, min_unit == Days);
                        if (min_unit == Days)
                            break;
                        [[fallthrough]];

                    case Hours:
                        processUnit(3600, 1, " hour", 5, value, buf_to, has_output, min_unit, min_unit == Hours);
                        if (min_unit == Hours)
                            break;
                        [[fallthrough]];

                    case Minutes:
                        processUnit(60, 1, " minute", 7, value, buf_to, has_output, min_unit, min_unit == Minutes);
                        if (min_unit == Minutes)
                            break;
                        [[fallthrough]];

                    case Seconds:
                        processUnit(1, 1, " second", 7, value, buf_to, has_output, min_unit, min_unit == Seconds);
                        if (min_unit == Seconds)
                            break;
                        [[fallthrough]];

                    case Milliseconds:
                        processUnit(1, 1000, " millisecond", 12, value, buf_to, has_output, min_unit, min_unit == Milliseconds);
                        if (min_unit == Milliseconds)
                            break;
                        [[fallthrough]];

                    case Microseconds:
                        processUnit(1, 1000000, " microsecond", 12, value, buf_to, has_output, min_unit, min_unit == Microseconds);
                        if (min_unit == Microseconds)
                            break;
                        [[fallthrough]];

                    case Nanoseconds:
                        processUnit(1, 1000000000, " nanosecond", 11, value, buf_to, has_output, min_unit, true);
                }
            }

            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        return col_to;
    }

    static void processUnit(
        UInt64 unit_multiplier, UInt64 unit_divisor, const char * unit_name, size_t unit_name_size,
        Float64 & value, WriteBuffer & buf_to, bool & has_output, Unit min_unit, bool is_minimum_unit)
    {
        if (unlikely(value + 1.0 == value))
        {
            /// The case when value is too large so exact representation for subsequent smaller units is not possible.
            writeText(std::floor(value * unit_divisor / unit_multiplier), buf_to);
            buf_to.write(unit_name, unit_name_size);
            writeChar('s', buf_to);
            has_output = true;
            value = 0;
            return;
        }
        UInt64 num_units;
        if (unit_divisor == 1)  /// dealing with whole number of seconds
        {
            num_units = static_cast<UInt64>(std::floor(value / unit_multiplier));

            if (!num_units && !is_minimum_unit)
            {
                /// Zero units, no need to print. But if it's the last (seconds) and the only unit, print "0 seconds" nevertheless.
                if (unit_multiplier != 1 || has_output)
                    return;
            }

            /// Remaining value to print on next iteration.
            value -= num_units * unit_multiplier;

            if (has_output)
            {
                /// Need delimiter between values. The last delimiter is " and ", all previous are comma.
                if ((value < 1 && min_unit >= Seconds) || is_minimum_unit)
                    writeCString(" and ", buf_to);
                else
                    writeCString(", ", buf_to);
            }
        }
        else   /// dealing with sub-seconds, a bit more peculiar to avoid more precision issues
        {
            Float64 shifted_unit_to_whole = value * unit_divisor;

            Float64 num_units_f;
            value = std::modf(shifted_unit_to_whole, &num_units_f);
            num_units = static_cast<UInt64>(std::llround(num_units_f));
            value /= unit_divisor;

            if (!num_units)
            {
                /// Zero units, no need to print. But if it's the last (nanoseconds) and the only unit, print "0 nanoseconds" nevertheless.
                if (!is_minimum_unit || has_output)
                    return;
            }

            if (has_output)
            {
                /// Need delimiter between values. The last delimiter is " and ", all previous are comma.
                if (is_minimum_unit || std::abs(value) <= 1E-9)
                    writeCString(" and ", buf_to);
                else
                    writeCString(", ", buf_to);
            }
        }

        writeText(num_units, buf_to);
        buf_to.write(unit_name, unit_name_size); /// If we just leave strlen(unit_name) here, clang-11 fails to make it compile-time.

        /// How to pronounce: unit vs. units.
        if (num_units != 1)
            writeChar('s', buf_to);

        has_output = true;
    }
};

}

REGISTER_FUNCTION(FormatReadableTimeDelta)
{
    factory.registerFunction<FunctionFormatReadableTimeDelta>();
}

}
