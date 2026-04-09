#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/Exception.h>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <cctype>
#include <fmt/format.h>
#include <stdexcept>

namespace DB
{

bool Bin::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;


    ++pos;

    String original_expr(pos->begin, pos->end);

    String value = getConvertedArgument(fn_name, pos);
    if (value.empty())
        return false;

    ++pos;
    String round_to = getConvertedArgument(fn_name, pos);
    round_to.erase(std::remove_if(round_to.begin(), round_to.end(), [](unsigned char c) { return std::isspace(c); }), round_to.end());
    if (round_to.empty())
        return false;

    String value_no_spaces = value;
    value_no_spaces.erase(std::remove_if(value_no_spaces.begin(), value_no_spaces.end(), [](unsigned char c) { return std::isspace(c); }), value_no_spaces.end());
    if (value_no_spaces.empty())
        return false;
    try
    {
        size_t pos_end = 0;
        std::stod(value_no_spaces, &pos_end);
        if (pos_end != value_no_spaces.size())
            throw std::invalid_argument("not a number");
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan value_tsp;
        if (value_tsp.parseConstKQLTimespan(value_no_spaces))
            value = std::to_string(value_tsp.toSeconds());
    }

    auto t = fmt::format("toFloat64({})", value);

    bool is_const_bin_size = false;
    try
    {
        bin_size = std::stod(round_to);
        is_const_bin_size = true;
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan time_span_parser;
        if (time_span_parser.parseConstKQLTimespan(round_to))
        {
            bin_size = time_span_parser.toSeconds();
            is_const_bin_size = true;
        }
    }

    if (is_const_bin_size && bin_size <= 0)
        return false;

    // Use datetime output whenever first argument is datetime/date (whether bin size is numeric or timespan)
    if (original_expr == "datetime" || original_expr == "date")
    {
        auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : round_to;
        auto inner = fmt::format("toDateTime64(toInt64({0}/{1}) * {1}, 9, 'UTC')", t, bin_sz);
        out = fmt::format("substring(replaceOne(toString({}), ' ', 'T'), 1, 27)", inner);
    }
    else if (original_expr == "timespan" || original_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(original_expr))
    {
        auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : round_to;
        String bin_value = fmt::format("toInt64({0}/{1}) * {1}", t, bin_sz);
        out = fmt::format(
            "concat(toString(toInt32((({}) as x) / 3600)),':', toString(toInt32(x % 3600 / 60)),':',toString(toInt32(x % 3600 % 60)))",
            bin_value);
    }
    else
    {
        auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : fmt::format("toFloat64({})", round_to);
        out = fmt::format("toInt64({0} / {1}) * {1}", t, bin_sz);
    }

    return true;
}

bool BinAt::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String original_expr(pos->begin, pos->end);

    String expression_str = getConvertedArgument(fn_name, pos);
    String expression_no_spaces = expression_str;
    expression_no_spaces.erase(std::remove_if(expression_no_spaces.begin(), expression_no_spaces.end(), [](unsigned char c) { return std::isspace(c); }), expression_no_spaces.end());
    if (expression_no_spaces.empty())
        return false;

    ++pos;
    String bin_size_str = getConvertedArgument(fn_name, pos);
    bin_size_str.erase(std::remove_if(bin_size_str.begin(), bin_size_str.end(), [](unsigned char c) { return std::isspace(c); }), bin_size_str.end());
    if (bin_size_str.empty())
        return false;

    ++pos;
    String fixed_point_str = getConvertedArgument(fn_name, pos);
    String fixed_point_no_spaces = fixed_point_str;
    fixed_point_no_spaces.erase(std::remove_if(fixed_point_no_spaces.begin(), fixed_point_no_spaces.end(), [](unsigned char c) { return std::isspace(c); }), fixed_point_no_spaces.end());
    if (fixed_point_no_spaces.empty())
        return false;

    auto t1 = fmt::format("toFloat64({})", fixed_point_str);
    auto t2 = fmt::format("toFloat64({})", expression_str);
    int dir = t2 >= t1 ? 0 : -1;

    try
    {
        bin_size = std::stod(bin_size_str);
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan time_span_parser;
        if (!time_span_parser.parseConstKQLTimespan(bin_size_str))
            return false;
        bin_size = time_span_parser.toSeconds();
    }

    // validate if bin_size is a positive number
    if (bin_size <= 0)
        return false;

    if (original_expr == "datetime" || original_expr == "date")
    {
        out = fmt::format("toDateTime64({} + toInt64(({} - {}) / {} + {}) * {}, 9, 'UTC')", t1, t2, t1, bin_size, dir, bin_size);
    }
    else if (original_expr == "timespan" || original_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(original_expr))
    {
        String bin_value = fmt::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
        out = fmt::format(
            "concat(toString(toInt32((({}) as x) / 3600)),':', toString(toInt32(x % 3600 / 60)), ':', toString(toInt32(x % 3600 % 60)))",
            bin_value);
    }
    else
    {
        out = fmt::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
    }
    return true;
}

bool Iif::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String predicate = getConvertedArgument(fn_name, pos);
    if (predicate.empty())
        return false;

    ++pos;
    String if_true = getConvertedArgument(fn_name, pos);
    if (if_true.empty())
        return false;

    ++pos;
    String if_false = getConvertedArgument(fn_name, pos);
    if (if_false.empty())
        return false;

    out = fmt::format("if({}, {}, {})", predicate, if_true, if_false);
    return true;
}

bool Iff::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String predicate = getConvertedArgument(fn_name, pos);
    if (predicate.empty())
        return false;

    ++pos;
    String if_true = getConvertedArgument(fn_name, pos);
    if (if_true.empty())
        return false;

    ++pos;
    String if_false = getConvertedArgument(fn_name, pos);
    if (if_false.empty())
        return false;

    out = fmt::format("if({}, {}, {})", predicate, if_true, if_false);
    return true;
}

bool Not::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    out = fmt::format("toBool(not({}))", arg);
    return true;
}

bool MinOf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "least");
}

bool MaxOf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "greatest");
}

bool Coalesce::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "coalesce");
}

bool Case::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "multiIf");
}

bool GeoDistance2Points::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String lon1 = getConvertedArgument(fn_name, pos);
    ++pos;
    String lat1 = getConvertedArgument(fn_name, pos);
    ++pos;
    String lon2 = getConvertedArgument(fn_name, pos);
    ++pos;
    String lat2 = getConvertedArgument(fn_name, pos);

    /// KQL returns NULL for invalid coordinates or NULL inputs
    out = fmt::format(
        "if(isNull({0}) or isNull({1}) or isNull({2}) or isNull({3}) or "
        "({0}) < -180 or ({0}) > 180 or ({1}) < -90 or ({1}) > 90 or "
        "({2}) < -180 or ({2}) > 180 or ({3}) < -90 or ({3}) > 90, "
        "NULL, geoDistance({0}, {1}, {2}, {3}))",
        lon1, lat1, lon2, lat2);
    return true;
}

bool GeoPointToGeohash::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String lon = getConvertedArgument(fn_name, pos);
    ++pos;
    String lat = getConvertedArgument(fn_name, pos);

    auto accuracy = getOptionalArgument(fn_name, pos);
    /// KQL default precision is 5 characters
    if (accuracy)
        out = fmt::format("geohashEncode({}, {}, {})", lon, lat, *accuracy);
    else
        out = fmt::format("geohashEncode({}, {}, 5)", lon, lat);

    return true;
}

bool ReplaceString::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "replaceAll");
}

bool URLEncodeComponent::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "encodeURLComponent");
}

bool PadLeft::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);
    ++pos;
    String total_len = getConvertedArgument(fn_name, pos);

    auto pad_char = getOptionalArgument(fn_name, pos);
    if (pad_char && *pad_char != "''")
        out = fmt::format("leftPad({}, {}, {})", source, total_len, *pad_char);
    else
        out = fmt::format("leftPad({}, {})", source, total_len);

    return true;
}

bool PadRight::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);
    ++pos;
    String total_len = getConvertedArgument(fn_name, pos);

    auto pad_char = getOptionalArgument(fn_name, pos);
    if (pad_char && *pad_char != "''")
        out = fmt::format("rightPad({}, {}, {})", source, total_len, *pad_char);
    else
        out = fmt::format("rightPad({}, {})", source, total_len);

    return true;
}

bool TrimWs::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "trimBoth");
}

bool ParseHex::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = getArgument(fn_name, pos);
    out = fmt::format("reinterpretAsInt64(reverse(unhex(replaceOne(toString({}), '0x', ''))))", argument);
    return true;
}

bool ToHex::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = getArgument(fn_name, pos);
    /// Remove leading zeros from hex output
    out = fmt::format("lower(trimLeft(hex(toInt64({})), '0'))", argument);
    return true;
}

bool IsAscii::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = getArgument(fn_name, pos);
    out = fmt::format("toBool(not(match(toString({}), '[^\\x00-\\x7F]')))", argument);
    return true;
}

bool IsUtf8::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    out = fmt::format("toBool(isValidUTF8({}))", arg);
    return true;
}

bool StrcatArray::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "arrayStringConcat");
}

bool DatetimeUtcToLocal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String datetime_arg = getConvertedArgument(fn_name, pos);
    ++pos;
    String timezone = getConvertedArgument(fn_name, pos);

    out = fmt::format("toTimeZone(toDateTime64({}, 9, 'UTC'), {})", datetime_arg, timezone);
    return true;
}

bool ToDateTimeFmt::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String datetime_str = getConvertedArgument(fn_name, pos);
    ++pos;
    String format_str = getConvertedArgument(fn_name, pos);

    out = fmt::format("parseDateTimeBestEffort({})", datetime_str);
    return true;
}

bool EndsWith::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "endsWith");
}

bool Any::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "any");
}

bool RowNumber::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto start_arg = getOptionalArgument(fn_name, pos);
    int64_t start = 1;
    if (start_arg)
    {
        try
        {
            start = std::stoll(*start_arg);
        }
        catch (...) {}
    }

    /// Optional second arg: reset flag (ignored for now - ClickHouse doesn't easily support reset)
    auto reset_arg = getOptionalArgument(fn_name, pos);
    (void)reset_arg;

    out = fmt::format("(row_number() OVER () + {} - 1)", start);
    return true;
}

bool Format::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    ++pos;
    String fmt_str = getConvertedArgument(fn_name, pos);

    if (fmt_str == "'x'")
        out = fmt::format("lower(trimLeft(hex(toInt64({})), '0'))", value);
    else if (fmt_str == "'X'")
        out = fmt::format("upper(trimLeft(hex(toInt64({})), '0'))", value);
    else
        out = fmt::format("toString({})", value);

    return true;
}

bool FormatInterp::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String fmt_str = getConvertedArgument(fn_name, pos);

    std::vector<String> args;
    while (auto arg = getOptionalArgument(fn_name, pos))
        args.push_back(*arg);

    String result = fmt_str;
    for (size_t i = 0; i < args.size(); ++i)
    {
        String placeholder_plain = fmt::format("{{{}:x}}", i);
        String placeholder_simple = fmt::format("{{{}}}", i);
        result = fmt::format("replaceAll({}, '{}', lower(trimLeft(hex(toInt64({})), '0')))", result, placeholder_plain, args[i]);
        result = fmt::format("replaceAll({}, '{}', toString({}))", result, placeholder_simple, args[i]);
    }

    out = result;
    return true;
}

}
