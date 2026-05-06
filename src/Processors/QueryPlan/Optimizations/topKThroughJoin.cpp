#include <Core/Joins.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{
/// Extract the JoinKind from either JoinStep (physical) or JoinStepLogical
/// (analyzer's logical plan, which is what we typically see in the first pass).
std::optional<JoinKind> getJoinKindFromStep(IQueryPlanStep * step)
{
    if (auto * physical = typeid_cast<JoinStep *>(step))
    {
        if (auto join_ptr = physical->getJoin())
            return join_ptr->getTableJoin().kind();
        return {};
    }
    if (auto * logical = typeid_cast<JoinStepLogical *>(step))
        return logical->getJoinOperator().kind;
    return {};
}
}

/// Push `Limit + Sort` down through a Join when the sort key only references
/// columns from the side preserved by the join (left of LEFT JOIN, right of RIGHT JOIN).
///
/// Soundness sketch
/// ----------------
/// Consider `Limit(n) <- Sort(K) <- Join(L, R)` where `K` only references columns from `L`
/// and the join is `LEFT` (so every L row produces at least one output row).
/// Output rows have K values exclusively drawn from L. The top-n rows by K of the join
/// output are therefore drawn from the rows of L that have the n largest (or smallest)
/// K values - that is, the top-n rows of L by K. Pre-sorting L by K and limiting to n
/// before the join restricts the set of L rows we expand without changing the final
/// top-n result. The outer Sort+Limit is preserved because LEFT JOIN may multiply
/// each L row into several output rows.
///
/// Mirror reasoning applies to RIGHT JOIN with K from R.
///
/// We do not apply this optimization to INNER joins: an L row with no R match produces
/// zero output rows, so limiting L to its top-n by K may cause every L survivor to drop
/// out, leaving fewer than n output rows even when the query has more.
///
/// Pattern matched: `LimitStep -> SortingStep -> [ExpressionStep] -> JoinStep`.
/// The optional ExpressionStep is allowed only when every sort key column passes
/// through it unchanged (i.e. the sort column name exists in the Expression's input
/// header with the same name). This keeps the column reference unambiguously attached
/// to one side of the join.
size_t tryTopKThroughJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// LIMIT WITH TIES needs to know how many rows have the threshold value, so
    /// we cannot stop reading early.
    if (limit_step->withTies())
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * sort_node = parent_node->children.front();
    auto * sort_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sort_step)
        return 0;

    /// Only Full sort is meaningful here. PartialSorting/MergingSorted indicate
    /// the input was already sorted, in which case there is nothing to push down.
    if (sort_step->getType() != SortingStep::Type::Full)
        return 0;

    if (sort_node->children.size() != 1)
        return 0;

    /// Peel a chain of ExpressionSteps between Sort and Join. We require that the sort
    /// columns are visible in each Expression's input header by the same name - otherwise
    /// the column may have been computed from join output rather than passed through from
    /// a single side. The cap of 4 is generous: in current plans the only steps between
    /// Sort and Join after `mergeExpressions` are `Before ORDER BY + Projection` and
    /// `Post Join Actions`, occasionally with one more wrapper.
    QueryPlan::Node * join_node = sort_node->children.front();
    for (size_t peeled = 0; peeled < 4; ++peeled)
    {
        auto * expression_step = typeid_cast<ExpressionStep *>(join_node->step.get());
        if (!expression_step)
            break;
        if (join_node->children.size() != 1)
            return 0;

        const auto & expr_input_header = expression_step->getInputHeaders().front();
        for (const auto & sort_col : sort_step->getSortDescription())
        {
            if (!expr_input_header->has(sort_col.column_name))
                return 0;
        }
        join_node = join_node->children.front();
    }

    auto join_kind_opt = getJoinKindFromStep(join_node->step.get());
    if (!join_kind_opt)
        return 0;
    if (join_node->children.size() != 2)
        return 0;

    const JoinKind join_kind = *join_kind_opt;

    size_t preserved_idx = 0;
    if (join_kind == JoinKind::Left)
        preserved_idx = 0;
    else if (join_kind == JoinKind::Right)
        preserved_idx = 1;
    else
        return 0;

    const auto & preserved_input_header = join_node->step->getInputHeaders().at(preserved_idx);
    const auto & other_input_header = join_node->step->getInputHeaders().at(1 - preserved_idx);

    /// All sort columns must be in the preserved side's input header, by the same
    /// name. Other names that may appear in the join output (right-side columns of a
    /// LEFT JOIN, etc.) come from the non-preserved side and would make the
    /// transformation unsound. We additionally require the column to NOT also appear
    /// on the other side: if both inputs carry a column with this name the analyzer
    /// would have renamed one, but defensively avoid ambiguity.
    for (const auto & sort_col : sort_step->getSortDescription())
    {
        if (!preserved_input_header->has(sort_col.column_name))
            return 0;
        if (other_input_header->has(sort_col.column_name))
            return 0;
    }

    /// `n` is the maximum number of L rows we need to consider on the preserved side.
    /// Any output row we keep after the outer LIMIT has its sort-key value drawn from
    /// one of the top-(limit+offset) L rows.
    const size_t n = limit_step->getLimitForSorting();
    if (n == 0)
        return 0;

    /// Reuse the cap that already gates `tryOptimizeTopK`. If the user disabled
    /// large-N TopK optimization there, do not work around it here.
    if (settings.max_limit_for_top_k_optimization && n > settings.max_limit_for_top_k_optimization)
        return 0;

    QueryPlan::Node * preserved_input_node = join_node->children.at(preserved_idx);

    /// Avoid re-applying: if the immediate child is already a LimitStep with a
    /// limit no larger than `n`, the optimization has already fired (or there is a
    /// user-supplied LIMIT we should not weaken).
    if (auto * existing_limit = typeid_cast<LimitStep *>(preserved_input_node->step.get()))
    {
        if (existing_limit->getLimit() <= n && existing_limit->getOffset() == 0)
            return 0;
    }

    /// Build `Limit(n) <- Sort(K, limit=n)` and graft it on top of the preserved input.
    auto new_sort_step = std::make_unique<SortingStep>(
        preserved_input_header,
        sort_step->getSortDescription(),
        n,
        sort_step->getSettings());

    auto & new_sort_node = nodes.emplace_back();
    new_sort_node.children.push_back(preserved_input_node);
    new_sort_node.step = std::move(new_sort_step);

    auto new_limit_step = std::make_unique<LimitStep>(
        new_sort_node.step->getOutputHeader(),
        n,
        /*offset_=*/ 0);

    auto & new_limit_node = nodes.emplace_back();
    new_limit_node.children.push_back(&new_sort_node);
    new_limit_node.step = std::move(new_limit_step);

    join_node->children[preserved_idx] = &new_limit_node;

    /// Re-run optimizations on the modified subtree so the inserted Sort+Limit can
    /// be picked up by tryOptimizeTopK / tryPushDownLimit etc.
    return 3;
}

}
