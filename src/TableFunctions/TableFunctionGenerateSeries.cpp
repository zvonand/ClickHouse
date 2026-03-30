#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INVALID_SETTING_VALUE;
}

namespace
{

constexpr std::array<const char *, 2> names = {"generate_series", "generateSeries"};

template <size_t alias_num>
class TableFunctionGenerateSeries : public ITableFunction
{
public:
    static_assert(alias_num < names.size());
    static constexpr auto name = names[alias_num];
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;
    const char * getStorageEngineName() const override
    {
        /// No underlying storage engine
        return "";
    }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};

template <size_t alias_num>
ColumnsDescription TableFunctionGenerateSeries<alias_num>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"generate_series", std::make_shared<DataTypeUInt64>()}}};
}

template <size_t alias_num>
StoragePtr TableFunctionGenerateSeries<alias_num>::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'length' or 'offset, length'.", getName());

        UInt64 start = evaluateArgument(context, arguments[0]);
        UInt64 stop = evaluateArgument(context, arguments[1]);

        /// Determine the step and whether the series is descending.
        /// Try Int64 first: if it succeeds and the value is negative, we have a descending series.
        /// If the value doesn't fit into Int64 (e.g. a large UInt64), fall back to UInt64 (always positive).
        /// This preserves backward compatibility for large positive UInt64 step values.
        bool negative_step = false;
        UInt64 abs_step = 1;
        if (arguments.size() == 3)
        {
            const auto & [field, type] = evaluateConstantExpression(arguments[2], context);

            if (!isNativeNumber(type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

            /// Try converting to Int64 first to detect negative values.
            Field as_signed = convertFieldToType(field, DataTypeInt64());
            if (!as_signed.isNull())
            {
                Int64 step_val = as_signed.safeGet<Int64>();
                if (step_val == 0)
                    throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Table function '{}' requires step to be a non-zero number", getName());
                if (step_val < 0)
                {
                    negative_step = true;
                    /// Compute absolute value without signed overflow: cast to unsigned before negation.
                    abs_step = UInt64(0) - static_cast<UInt64>(step_val);
                }
                else
                {
                    abs_step = static_cast<UInt64>(step_val);
                }
            }
            else
            {
                /// Value too large for Int64 — must be a large positive UInt64.
                Field as_unsigned = convertFieldToType(field, DataTypeUInt64());
                if (as_unsigned.isNull())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The value {} is not representable as UInt64",
                        applyVisitor(FieldVisitorToString(), field));

                abs_step = as_unsigned.safeGet<UInt64>();
                if (abs_step == 0)
                    throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Table function '{}' requires step to be a non-zero number", getName());
            }
        }

        /// Compute the number of stepped values in a descending series, guarding against UInt64 overflow.
        /// Used only for negative step, where `DescendingNumbersSource` needs the actual value count.
        /// The formula is `range / abs_step + 1`, but `+ 1` can wrap to 0 when `range / abs_step == UInt64_MAX`.
        auto computeCardinality = [&](UInt64 range) -> UInt64
        {
            UInt64 quotient = range / abs_step;
            if (quotient == std::numeric_limits<UInt64>::max())
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Table function '{}' produces too many values (cardinality overflows UInt64)",
                    getName());
            return quotient + 1;
        };

        StoragePtr res;
        if (!negative_step)
        {
            if (start > stop)
            {
                res = std::make_shared<StorageSystemNumbers>(
                    StorageID(getDatabaseName(), table_name), false, std::string{"generate_series"}, 0, 0, 1);
            }
            else
            {
                /// The limit parameter of StorageSystemNumbers is the raw domain window size
                /// (number of consecutive integers), not the number of stepped values.
                /// ReadFromSystemNumbersStep handles the stepping internally.
                res = std::make_shared<StorageSystemNumbers>(
                    StorageID(getDatabaseName(), table_name), false, std::string{"generate_series"}, (stop - start) + 1, start, abs_step);
            }
        }
        else
        {
            /// Negative step: generate descending series from start down to stop.
            if (start < stop)
            {
                res = std::make_shared<StorageSystemNumbers>(
                    StorageID(getDatabaseName(), table_name), false, std::string{"generate_series"}, 0, 0, 1);
            }
            else
            {
                UInt64 count = computeCardinality(start - stop);
                res = std::make_shared<StorageSystemNumbers>(
                    StorageID(getDatabaseName(), table_name), false, std::string{"generate_series"},
                    count, start, abs_step, /* descending= */ true);
            }
        }
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit' or 'offset, limit'.", getName());
}

template <size_t alias_num>
UInt64 TableFunctionGenerateSeries<alias_num>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The value {} is not representable as UInt64",
            applyVisitor(FieldVisitorToString(), field));

    return converted.safeGet<UInt64>();
}

}

void registerTableFunctionGenerateSeries(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateSeries<0>>({}, {.allow_readonly = true});
    factory.registerFunction<TableFunctionGenerateSeries<1>>({}, {.allow_readonly = true});
}

}
