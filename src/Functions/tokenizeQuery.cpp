#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Lexer.h>


namespace DB
{

namespace
{

DataTypePtr makeTokenTypeEnum()
{
    return std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
#define M(TOKEN) {#TOKEN, static_cast<Int8>(TokenType::TOKEN)},
    APPLY_FOR_TOKENS(M)
#undef M
    });
}

class FunctionTokenizeQuery : public IFunction
{
public:
    static constexpr auto name = "tokenizeQuery";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTokenizeQuery>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, args);

        DataTypes types{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>(), makeTokenTypeEnum()};
        Strings names{"begin", "end", "type"};
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(types), std::move(names)));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString & col_query = assert_cast<const ColumnString &>(*arguments[0].column);

        auto col_begin = ColumnUInt64::create();
        auto col_end = ColumnUInt64::create();
        auto col_type = ColumnInt8::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto & data_begin = col_begin->getData();
        auto & data_end = col_end->getData();
        auto & data_type = col_type->getData();
        auto & offsets = col_offsets->getData();
        offsets.resize(input_rows_count);

        size_t total_tokens = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query = col_query.getDataAt(i);
            const char * begin = query.data();
            const char * end = begin + query.size();

            Lexer lexer(begin, end);
            while (true)
            {
                Token token = lexer.nextToken();
                if (token.isEnd())
                    break;

                data_begin.push_back(token.begin - begin);
                data_end.push_back(token.end - begin);
                data_type.push_back(static_cast<Int8>(token.type));
                ++total_tokens;
            }

            offsets[i] = total_tokens;
        }

        MutableColumns tuple_columns;
        tuple_columns.emplace_back(std::move(col_begin));
        tuple_columns.emplace_back(std::move(col_end));
        tuple_columns.emplace_back(std::move(col_type));

        return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(col_offsets));
    }
};

}

REGISTER_FUNCTION(TokenizeQuery)
{
    factory.registerFunction<FunctionTokenizeQuery>(FunctionDocumentation{
        .description = R"(
Tokenizes a ClickHouse SQL query string and returns an array of tokens.
Each token is a named tuple with the beginning position (in bytes), the end position, and the token type.
)",
        .syntax = "tokenizeQuery(query)",
        .arguments = {{"query", "A ClickHouse SQL query string. String."}},
        .returned_value = {"An array of named tuples `(begin UInt64, end UInt64, type Enum8(...))` representing the tokens of the query.", {"Array(Tuple(begin UInt64, end UInt64, type Enum8(...)))"}},
        .examples = {{"simple", "SELECT tokenizeQuery('SELECT 1')", R"([('0','6','BareWord'),('6','7','Whitespace'),('7','8','Number')])"}},
        .category = FunctionDocumentation::Category::Other,
    });
}

}
