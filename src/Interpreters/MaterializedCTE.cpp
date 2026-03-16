#include <Interpreters/MaterializedCTE.h>

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

MaterializedCTE::MaterializedCTE(const StoragePtr & storage_, const std::string & cte_name_)
    : storage(storage_)
    , cte_name(cte_name_)
{}

MaterializedCTE::~MaterializedCTE() noexcept = default;

}
