#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLTop.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>

#include <fmt/format.h>

namespace DB
{

bool ParserKQLTop::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// top N by column [asc|desc]
    /// e.g., top 3 by LineCount

    if (!isValidKQLPos(pos))
        return false;

    /// Get N (limit) — resolve let bindings
    String limit_str(pos->begin, pos->end);
    auto & bindings = kqlLetBindings();
    if (auto it = bindings.find(limit_str); it != bindings.end())
        limit_str = it->second;
    ++pos;

    /// Expect "by"
    if (!isValidKQLPos(pos))
        return false;

    String by_token(pos->begin, pos->end);
    if (by_token != "by")
        return false;
    ++pos;

    /// Get the sort expression (rest of the pipe)
    auto sort_expr = getExprFromPipe(pos);
    if (sort_expr.empty())
        return false;

    /// Parse order by
    ParserOrderByExpressionList order_list;
    ASTPtr order_expression_list;

    Tokens sort_tokens(sort_expr.data(), sort_expr.data() + sort_expr.size(), 0, true);
    IParser::Pos sort_pos(sort_tokens, pos.max_depth, pos.max_backtracks);

    if (!order_list.parse(sort_pos, order_expression_list, expected))
        return false;

    /// Default to desc if no direction was explicitly specified.
    /// We scan tokens for "asc"/"desc" keywords rather than using substring search
    /// to avoid false positives (e.g. a column named "description").
    {
        bool has_explicit_dir = false;
        Tokens check_tokens(sort_expr.data(), sort_expr.data() + sort_expr.size(), 0, true);
        IParser::Pos check_pos(check_tokens, pos.max_depth, pos.max_backtracks);
        while (isValidKQLPos(check_pos))
        {
            String tok(check_pos->begin, check_pos->end);
            if (check_pos->type == TokenType::BareWord && (tok == "asc" || tok == "desc"))
            {
                has_explicit_dir = true;
                break;
            }
            ++check_pos;
        }
        if (!has_explicit_dir)
        {
            for (auto & child : order_expression_list->children)
            {
                auto * order_expr = child->as<ASTOrderByElement>();
                order_expr->direction = -1;
                if (!order_expr->nulls_direction_was_explicitly_specified)
                    order_expr->nulls_direction = -1;
            }
        }
    }

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));

    /// Set limit
    ASTPtr limit_length;
    Tokens limit_tokens(limit_str.data(), limit_str.data() + limit_str.size(), 0, true);
    IParser::Pos limit_pos(limit_tokens, pos.max_depth, pos.max_backtracks);

    if (!ParserExpressionWithOptionalAlias(false).parse(limit_pos, limit_length, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));

    return true;
}

}
