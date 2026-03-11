#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>

namespace DB
{

struct LocalMetaCommandResult
{
    enum class Kind
    {
        NotMatched,
        RewriteQuery,
    };

    Kind kind = Kind::NotMatched;
    String query;
};

/*
Try to handle a clickhouse-local meta-command.

- NotMatched: input is not a supported local meta-command
- RewriteQuery: replace input with `query` and continue normal processing
*/
LocalMetaCommandResult tryHandleLocalMetaCommand(std::string_view input);

}
