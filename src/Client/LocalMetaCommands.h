#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>

namespace DB
{

/**
 * This struct represents the Result of a treated command coming from clickhouse-local
 * - LocalMetaCommandResult::Kind
 *  - NotMatched: input is not a supported local meta-command
 *  - RewriteQuery: replace input with `query` and continue normal processing
 * When the LocalMetaCommandResult is a RewriteQuery, the rewritten query will be stored in the member query
 * New Kinds can be added and handled accordingly
 */
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
*/
LocalMetaCommandResult tryHandleLocalMetaCommand(std::string_view input);

}
