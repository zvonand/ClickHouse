#pragma once

#include "config.h"

#if USE_AVRO

#include <functional>
#include <limits>
#include <map>
#include <vector>

#include <Core/Field.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB::Iceberg
{

/// Generic field-level parsers reusable across Iceberg EXECUTE commands.
Int64 parseInt64Field(const Field & value, std::string_view command_name, std::string_view arg_name);
bool parseBoolField(const Field & value, std::string_view command_name, std::string_view arg_name);

/// General-purpose argument parser for Iceberg EXECUTE commands.
///
/// Usage pattern:
///   1. Create a parser with the command name.
///   2. Register positional argument handlers via addPositional().
///   3. Register named (key = value) argument handlers via addNamedArg().
///   4. Register post-parse constraint validators via addConstraint().
///   5. Call parse() with the AST arguments node.
///
/// The parser handles:
///   - Splitting positional arguments from key=value arguments.
///   - Rejecting extra positional arguments beyond registered count.
///   - Rejecting unknown named arguments with a helpful error.
///   - Calling constraint validators after all arguments are processed.
class IcebergCommandArgumentsParser
{
public:
    using PositionalHandler = std::function<void(const ASTPtr &)>;
    using NamedHandler = std::function<void(const Field &)>;
    using Validator = std::function<void()>;

    explicit IcebergCommandArgumentsParser(String command_name_);

    void addPositional(PositionalHandler handler);
    void addNamedArg(const String & name, NamedHandler handler);
    void addConstraint(Validator validator);

    void parse(const ASTPtr & args, ContextPtr context) const;

private:
    String command_name;
    std::vector<PositionalHandler> positional_handlers;
    std::map<String, NamedHandler> named_handlers;
    std::vector<Validator> constraints;
};

}

#endif
