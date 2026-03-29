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
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/TokenIterator.h>


namespace DB
{

namespace
{

DataTypePtr makeHighlightTypeEnum()
{
    return std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"none", static_cast<Int8>(Highlight::none)},
        {"keyword", static_cast<Int8>(Highlight::keyword)},
        {"identifier", static_cast<Int8>(Highlight::identifier)},
        {"function", static_cast<Int8>(Highlight::function)},
        {"alias", static_cast<Int8>(Highlight::alias)},
        {"substitution", static_cast<Int8>(Highlight::substitution)},
        {"number", static_cast<Int8>(Highlight::number)},
        {"string", static_cast<Int8>(Highlight::string)},
        {"string_escape", static_cast<Int8>(Highlight::string_escape)},
        {"string_metacharacter", static_cast<Int8>(Highlight::string_metacharacter)},
    });
}

class FunctionHighlightQuery : public IFunction
{
public:
    static constexpr auto name = "highlightQuery";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHighlightQuery>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, args);

        DataTypes types{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>(), makeHighlightTypeEnum()};
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

        size_t total_ranges = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query = col_query.getDataAt(i);
            const char * begin = query.data();
            const char * end = begin + query.size();

            Tokens tokens(begin, end, /* max_query_size = */ 0, /* skip_insignificant = */ true);
            IParser::Pos token_iterator(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

            Expected expected;
            expected.enable_highlighting = true;

            ParserQuery parser(end, /* allow_settings_after_format_in_insert = */ false, /* implicit_select = */ false);
            ASTPtr ast;

            try
            {
                while (!token_iterator->isEnd())
                {
                    bool res = parser.parse(token_iterator, ast, expected);
                    if (!res)
                        break;

                    if (!token_iterator->isEnd() && token_iterator->type != TokenType::Semicolon)
                        break;

                    while (token_iterator->type == TokenType::Semicolon)
                        ++token_iterator;
                }
            }
            catch (...)
            {
                /// Skip highlighting on parse errors, just return what we have so far for this row.
            }

            const auto expanded = expandHighlights(expected.highlights);

            for (const auto & range : expanded)
            {
                data_begin.push_back(range.begin - begin);
                data_end.push_back(range.end - begin);
                data_type.push_back(static_cast<Int8>(range.highlight));
                ++total_ranges;
            }

            offsets[i] = total_ranges;
        }

        MutableColumns tuple_columns;
        tuple_columns.emplace_back(std::move(col_begin));
        tuple_columns.emplace_back(std::move(col_end));
        tuple_columns.emplace_back(std::move(col_type));

        return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(col_offsets));
    }
};

}

REGISTER_FUNCTION(HighlightQuery)
{
    factory.registerFunction<FunctionHighlightQuery>(FunctionDocumentation{
        .description = R"(
Parses a ClickHouse SQL query string and returns an array of highlighted ranges for syntax highlighting.
Each range is a named tuple with the beginning position (in bytes), the end position, and the highlight type.
The highlight types describe the syntactic role of the fragment (keyword, identifier, function, etc.)
and can be used to assign colors in a UI. Inside LIKE and REGEXP string patterns, metacharacters
and escape characters are highlighted separately.
)",
        .syntax = "highlightQuery(query)",
        .arguments = {{"query", "A ClickHouse SQL query string. String."}},
        .returned_value = {"An array of named tuples `(begin UInt64, end UInt64, type Enum8(...))` representing highlighted ranges.", {"Array(Tuple(begin UInt64, end UInt64, type Enum8(...)))"}},
        .examples = {{"simple", "SELECT highlightQuery('SELECT 1')", R"([('0','6','keyword'),('7','8','number')])"}},
        .category = FunctionDocumentation::Category::Other,
    });
}

}
