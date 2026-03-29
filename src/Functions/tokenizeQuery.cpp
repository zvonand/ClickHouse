#include <Functions/FunctionFactory.h>
#include <Functions/QueryTokenizationImpl.h>
#include <Parsers/Lexer.h>


namespace DB
{

namespace
{

struct TokenizeQueryImpl
{
    static constexpr auto name = "tokenizeQuery";

    static DataTypePtr makeEnumType()
    {
        return std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
#define M(TOKEN) {#TOKEN, static_cast<Int8>(TokenType::TOKEN)},
            APPLY_FOR_TOKENS(M)
#undef M
        });
    }

    static void processRow(
        std::string_view query, const char * begin,
        PaddedPODArray<UInt64> & data_begin,
        PaddedPODArray<UInt64> & data_end,
        PaddedPODArray<Int8> & data_type,
        size_t & total)
    {
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
            ++total;
        }
    }
};

}

REGISTER_FUNCTION(TokenizeQuery)
{
    factory.registerFunction<FunctionQueryTokenization<TokenizeQueryImpl>>(FunctionDocumentation{
        .description = R"(
Tokenizes a ClickHouse SQL query string and returns an array of tokens.
Each token is a named tuple with the beginning position (in bytes), the end position, and the token type.
)",
        .syntax = "tokenizeQuery(query)",
        .arguments = {{"query", "A ClickHouse SQL query string. String."}},
        .returned_value = {"An array of named tuples `(begin UInt64, end UInt64, type Enum8(...))` representing the tokens of the query.", {"Array(Tuple(begin UInt64, end UInt64, type Enum8(...)))"}},
        .examples = {{"simple", "SELECT tokenizeQuery('SELECT 1')", R"([('0','6','BareWord'),('6','7','Whitespace'),('7','8','Number')])"}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::Other,
    });
}

}
