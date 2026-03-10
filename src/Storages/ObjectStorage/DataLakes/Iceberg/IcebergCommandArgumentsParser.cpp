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

Int64 parseInt64Field(const Field & value, std::string_view command_name, std::string_view arg_name)
{
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>();
    if (value.getType() == Field::Types::UInt64)
    {
        UInt64 value_uint = value.safeGet<UInt64>();
        if (value_uint > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' is too large: {}", command_name, arg_name, value_uint);
        return static_cast<Int64>(value_uint);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be an integer literal", command_name, arg_name);
}

bool parseBoolField(const Field & value, std::string_view command_name, std::string_view arg_name)
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

IcebergCommandArgumentsParser::IcebergCommandArgumentsParser(String command_name_)
    : command_name(std::move(command_name_))
{
}

void IcebergCommandArgumentsParser::addPositional(PositionalHandler handler)
{
    positional_handlers.push_back(std::move(handler));
}

void IcebergCommandArgumentsParser::addNamedArg(const String & name, NamedHandler handler)
{
    named_handlers.emplace(name, std::move(handler));
}

void IcebergCommandArgumentsParser::addConstraint(Validator validator)
{
    constraints.push_back(std::move(validator));
}

void IcebergCommandArgumentsParser::parse(const ASTPtr & args, ContextPtr context) const
{
    if (!args)
        return;

    ASTs all_args = args->children;
    auto first_kv_it = getFirstKeyValueArgument(all_args);
    size_t positional_count = static_cast<size_t>(std::distance(all_args.begin(), first_kv_it));

    if (positional_count > positional_handlers.size())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{} expects at most {} positional argument(s), got {}",
            command_name,
            positional_handlers.size(),
            positional_count);

    for (size_t i = 0; i < positional_count; ++i)
        positional_handlers[i](all_args[i]);

    ASTs kv_args(first_kv_it, all_args.end());
    auto parsed_kv = parseKeyValueArguments(kv_args, context);

    for (const auto & [key, value] : parsed_kv)
    {
        auto it = named_handlers.find(key);
        if (it == named_handlers.end())
        {
            String supported;
            for (const auto & [arg_name, _] : named_handlers)
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
        it->second(value);
    }

    for (const auto & constraint : constraints)
        constraint();
}

}
}

#endif
