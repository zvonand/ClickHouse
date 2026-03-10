#pragma once

#include "config.h"

#if USE_AVRO

#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <vector>

#include <Core/Field.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB::Iceberg
{

/// Supported argument types for declarative registration.
enum class ArgType
{
    Int64,
    Bool,
    String,
    Array,
    Field,
};

/// Immutable result returned by IcebergCommandArgumentsParser::parse().
class ParsedArguments
{
public:
    bool has(const String & name) const;

    Int64 getInt64(const String & name) const;
    bool getBool(const String & name) const;
    String getString(const String & name) const;
    const Array & getArray(const String & name) const;
    const DB::Field & getField(const String & name) const;

    std::optional<Int64> tryGetInt64(const String & name) const;
    std::optional<bool> tryGetBool(const String & name) const;
    std::optional<String> tryGetString(const String & name) const;

    const std::vector<DB::Field> & positional() const { return positional_values; }

private:
    friend class IcebergCommandArgumentsParser;

    std::map<String, DB::Field> named_values;
    std::vector<DB::Field> positional_values;
    String command_name;
};

/// General-purpose argument parser for Iceberg EXECUTE commands.
///
/// Usage pattern (declarative):
///   1. Create a parser with the command name.
///   2. Register positional argument count via addPositional().
///   3. Register named arguments via addNamedArg(name, type).
///   4. Register post-parse constraint validators via addConstraint().
///   5. Call parse() -- returns a ParsedArguments with typed getters.
///
/// For arguments that need custom transformation (e.g. duration strings
/// to milliseconds), use addNamedArg with ArgType::Field or ArgType::String
/// and transform after parse().
class IcebergCommandArgumentsParser
{
public:
    using Validator = std::function<void(const ParsedArguments &)>;

    explicit IcebergCommandArgumentsParser(String command_name_);

    /// Register that the command accepts a positional argument.
    /// Positional arguments are returned as raw Fields in ParsedArguments::positional().
    void addPositional();

    /// Register a named argument with a declared type.
    /// The parser will validate that the provided value matches the type.
    void addNamedArg(const String & name, ArgType type);

    /// Register a post-parse constraint that receives the parsed result.
    void addConstraint(Validator validator);

    /// Parse the AST arguments and return a typed result.
    ParsedArguments parse(const ASTPtr & args, ContextPtr context) const;

    const String & commandName() const { return command_name; }

private:
    DB::Field convertField(const String & name, ArgType type, const DB::Field & raw) const;

    String command_name;
    size_t positional_count = 0;
    std::map<String, ArgType> named_args;
    std::vector<Validator> constraints;
};

}

#endif
