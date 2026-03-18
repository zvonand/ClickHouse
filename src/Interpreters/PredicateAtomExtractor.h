#pragma once

#include <Interpreters/PredicateAtom.h>
#include <Interpreters/ActionsDAG.h>
#include <vector>


namespace DB
{

/// uses ActionsDAG::extractConjunctionAtoms to decompose AND chains
/// then classifies each atom by inspecting `function_base->getName`
std::vector<PredicateAtom> extractPredicateAtoms(const ActionsDAG::Node * filter_node);

/// classify function name into a predicate_class
String classifyPredicateFunction(const String & function_name);

}
