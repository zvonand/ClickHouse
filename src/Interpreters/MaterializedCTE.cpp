#include <Interpreters/MaterializedCTE.h>

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

MaterializedCTE::MaterializedCTE(const std::string & cte_name_)
    : cte_name(cte_name_)
{}

MaterializedCTE::~MaterializedCTE() noexcept = default;

}
