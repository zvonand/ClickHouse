#include "config.h"
#if USE_AVRO

#include <cctype>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergCommandArgumentsParser.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Iceberg
{

namespace
{

Int64 fieldToInt64(const Field & value, std::string_view command_name, std::string_view arg_name)
{
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>();
    if (value.getType() == Field::Types::UInt64)
    {
        UInt64 v = value.safeGet<UInt64>();
        if (v > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' is too large: {}", command_name, arg_name, v);
        return static_cast<Int64>(v);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be an integer literal", command_name, arg_name);
}

bool fieldToBool(const Field & value, std::string_view command_name, std::string_view arg_name)
{
    if (value.getType() == Field::Types::Bool)
        return value.safeGet<bool>();
    if (value.getType() == Field::Types::UInt64)
        return value.safeGet<UInt64>() != 0;
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>() != 0;
    if (value.getType() == Field::Types::String)
    {
        String lower = value.safeGet<String>();
        for (auto & ch : lower)
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));

        if (lower == "true")
            return true;
        if (lower == "false")
            return false;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be a boolean or integer literal", command_name, arg_name);
}

}

/// ---------- ParsedArguments ----------

bool ParsedArguments::has(const String & name) const
{
    return named_values.contains(name);
}

Int64 ParsedArguments::getInt64(const String & name) const
{
    return named_values.at(name).safeGet<Int64>();
}

bool ParsedArguments::getBool(const String & name) const
{
    return named_values.at(name).safeGet<bool>();
}

String ParsedArguments::getString(const String & name) const
{
    return named_values.at(name).safeGet<String>();
}

const Array & ParsedArguments::getArray(const String & name) const
{
    return named_values.at(name).safeGet<Array>();
}

const DB::Field & ParsedArguments::getField(const String & name) const
{
    return named_values.at(name);
}

std::optional<Int64> ParsedArguments::tryGetInt64(const String & name) const
{
    auto it = named_values.find(name);
    if (it == named_values.end())
        return std::nullopt;
    return it->second.safeGet<Int64>();
}

std::optional<bool> ParsedArguments::tryGetBool(const String & name) const
{
    auto it = named_values.find(name);
    if (it == named_values.end())
        return std::nullopt;
    return it->second.safeGet<bool>();
}

std::optional<String> ParsedArguments::tryGetString(const String & name) const
{
    auto it = named_values.find(name);
    if (it == named_values.end())
        return std::nullopt;
    return it->second.safeGet<String>();
}

/// ---------- IcebergCommandArgumentsParser ----------

IcebergCommandArgumentsParser::IcebergCommandArgumentsParser(String command_name_)
    : command_name(std::move(command_name_))
{
}

void IcebergCommandArgumentsParser::addPositional()
{
    ++positional_count;
}

void IcebergCommandArgumentsParser::addNamedArg(const String & name, ArgType type)
{
    named_args.emplace(name, type);
}

void IcebergCommandArgumentsParser::addConstraint(Validator validator)
{
    constraints.push_back(std::move(validator));
}

Field IcebergCommandArgumentsParser::convertField(const String & name, ArgType type, const Field & raw) const
{
    switch (type)
    {
        case ArgType::Int64:
            return Field(fieldToInt64(raw, command_name, name));

        case ArgType::Bool:
            return Field(fieldToBool(raw, command_name, name));

        case ArgType::String:
        {
            if (raw.getType() != Field::Types::String)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be a string literal", command_name, name);
            return raw;
        }

        case ArgType::Array:
        {
            if (raw.getType() != Field::Types::Array)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be an array literal", command_name, name);
            return raw;
        }

        case ArgType::Field:
            return raw;
    }
    UNREACHABLE();
}

ParsedArguments IcebergCommandArgumentsParser::parse(const ASTPtr & args, ContextPtr context) const
{
    ParsedArguments result;
    result.command_name = command_name;

    if (!args)
        return result;

    ASTs all_args = args->children;
    auto first_kv_it = getFirstKeyValueArgument(all_args);
    size_t pos_count = static_cast<size_t>(std::distance(all_args.begin(), first_kv_it));

    if (pos_count > positional_count)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{} expects at most {} positional argument(s), got {}",
            command_name,
            positional_count,
            pos_count);

    for (size_t i = 0; i < pos_count; ++i)
    {
        auto * lit = all_args[i]->as<ASTLiteral>();
        if (lit)
            result.positional_values.push_back(lit->value);
        else
            result.positional_values.push_back(Field(all_args[i]->getColumnName()));
    }

    ASTs kv_args(first_kv_it, all_args.end());
    auto parsed_kv = parseKeyValueArguments(kv_args, context);

    for (const auto & [key, value] : parsed_kv)
    {
        auto it = named_args.find(key);
        if (it == named_args.end())
        {
            String supported;
            for (const auto & [arg_name, _] : named_args)
            {
                if (!supported.empty())
                    supported += ", ";
                supported += arg_name;
            }
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown {} argument '{}'. Supported: {}",
                command_name,
                key,
                supported);
        }
        result.named_values[key] = convertField(key, it->second, value);
    }

    for (const auto & constraint : constraints)
        constraint(result);

    return result;
}

}
}

#endif
