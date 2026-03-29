#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}


/// arrayTranspose([[1, 2, 3], [4, 5, 6]]) = [[1, 4], [2, 5], [3, 6]]
class FunctionArrayTranspose : public IFunction
{
public:
    static constexpr auto name = "arrayTranspose";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayTranspose>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * outer_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!outer_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Array(Array(T)), got {}",
                getName(), arguments[0]->getName());

        if (!checkAndGetDataType<DataTypeArray>(outer_type->getNestedType().get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Array(Array(T)), got {}",
                getName(), arguments[0]->getName());

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * outer_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!outer_col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} in argument of function {}",
                arguments[0].column->getName(), getName());

        const ColumnArray * inner_col = checkAndGetColumn<ColumnArray>(&outer_col->getData());
        if (!inner_col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} in argument of function {}",
                outer_col->getData().getName(), getName());

        const IColumn & src_data = inner_col->getData();
        const ColumnArray::Offsets & outer_offsets = outer_col->getOffsets();
        const ColumnArray::Offsets & inner_offsets = inner_col->getOffsets();

        /// Validate that all inner arrays in each row have equal size, and compute the total
        /// number of result inner arrays (sum of inner_size across rows) for exact reservation.
        size_t total_inner_size = 0;
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            /// -1 array subscript is Ok, see PaddedPODArray
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];

            if (outer_start == outer_end)
                continue;

            size_t inner_size = inner_offsets[outer_start] - inner_offsets[outer_start - 1];
            for (ColumnArray::Offset j = outer_start + 1; j < outer_end; ++j)
            {
                size_t current_inner_size = inner_offsets[j] - inner_offsets[j - 1];
                if (current_inner_size != inner_size)
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "All inner arrays in argument of function {} must have equal sizes, "
                        "but row {} has inner arrays of size {} and {}",
                        getName(), i, inner_size, current_inner_size);
            }
            total_inner_size += inner_size;
        }

        auto result_outer_offsets_col = ColumnArray::ColumnOffsets::create();
        auto result_inner_offsets_col = ColumnArray::ColumnOffsets::create();
        auto result_data = src_data.cloneEmpty();

        ColumnArray::Offsets & result_outer_offsets = result_outer_offsets_col->getData();
        ColumnArray::Offsets & result_inner_offsets = result_inner_offsets_col->getData();

        result_outer_offsets.reserve(input_rows_count);
        result_inner_offsets.reserve(total_inner_size);
        result_data->reserve(src_data.size());

        ColumnArray::Offset result_outer_offset = 0;
        ColumnArray::Offset result_inner_offset = 0;

        /// Transpose the matrix per row: output[j][k] = input[k][j].
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];

            /// Number of inner arrays (number of rows in the input matrix)
            size_t outer_size = outer_end - outer_start;

            /// Size of each inner array (number of columns in the input matrix), validated in the first loop
            size_t inner_size = outer_size > 0 ? inner_offsets[outer_start] - inner_offsets[outer_start - 1] : 0;

            for (size_t j = 0; j < inner_size; ++j)
            {
                for (ColumnArray::Offset k = outer_start; k < outer_end; ++k)
                    result_data->insertFrom(src_data, inner_offsets[k - 1] + j);

                result_inner_offset += outer_size;
                result_inner_offsets.push_back(result_inner_offset);
            }

            result_outer_offset += inner_size;
            result_outer_offsets.push_back(result_outer_offset);
        }

        auto result_inner_array = ColumnArray::create(std::move(result_data), std::move(result_inner_offsets_col));
        return ColumnArray::create(std::move(result_inner_array), std::move(result_outer_offsets_col));
    }
};


REGISTER_FUNCTION(ArrayTranspose)
{
    FunctionDocumentation::Description description = R"(
Transposes a two-dimensional array.

All inner arrays must have the same length.
)";
    FunctionDocumentation::Syntax syntax = "arrayTranspose(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "A two-dimensional array to transpose. All inner arrays must have the same length.", {"Array(Array(T))"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "A transposed two-dimensional array where element `[i][j]` of the result equals element `[j][i]` of the input.",
        {"Array(Array(T))"}
    };
    FunctionDocumentation::Examples examples = {
        {"Square matrix", "SELECT arrayTranspose([[1, 2], [3, 4]])", "[[1,3],[2,4]]"},
        {"Non-square matrix", "SELECT arrayTranspose([[1, 2, 3], [4, 5, 6]])", "[[1,4],[2,5],[3,6]]"},
        {"String elements", "SELECT arrayTranspose([['a', 'b'], ['c', 'd']])", "[['a','c'],['b','d']]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayTranspose>(documentation);
}

}
