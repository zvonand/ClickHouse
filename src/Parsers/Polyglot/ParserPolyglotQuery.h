#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
// Even when polyglot is disabled, it is not possible to exclude this parser because changing the dialect via `SET dialect = '...'` queries should succeed.
class ParserPolyglotQuery final : public IParserBase
{
private:
    [[maybe_unused]] size_t max_query_size;
    [[maybe_unused]] size_t max_parser_depth;
    [[maybe_unused]] size_t max_parser_backtracks;
    [[maybe_unused]] String source_dialect;

public:
    ParserPolyglotQuery(size_t max_query_size_, size_t max_parser_depth_, size_t max_parser_backtracks_, const String & source_dialect_)
        : max_query_size(max_query_size_)
        , max_parser_depth(max_parser_depth_)
        , max_parser_backtracks(max_parser_backtracks_)
        , source_dialect(source_dialect_)
    {
    }

    const char * getName() const override { return "Polyglot SQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
