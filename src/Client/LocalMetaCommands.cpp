#include <Client/LocalMetaCommands.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace
{

// Helper function to normalize the input and avoid issues
// Accepts formats with and without ';'
String normalizeMetaCommand(std::string_view input)
{
    String s(input);
    boost::algorithm::trim(s);

    if (!s.empty() && s.back() == ';')
    {
        s.pop_back();
        boost::algorithm::trim(s);
    }

    return s;
}

// Rewrites command LS into a query that produces the list of all files of the current directory
std::optional<String> rewriteLS(std::string_view input)
{
    const String normalized = normalizeMetaCommand(input);

    if (!boost::iequals(normalized, "ls"))
        return std::nullopt;

    return "SELECT _file AS file FROM file('*', 'One') ORDER BY file";
}

}

/// Dispatcher for all local meta-commands.
LocalMetaCommandResult tryHandleLocalMetaCommand(std::string_view input)
{
    if (auto rewritten = rewriteLS(input))
    {
        return {LocalMetaCommandResult::Kind::RewriteQuery, std::move(*rewritten)};
    }

    return {};
}

}
