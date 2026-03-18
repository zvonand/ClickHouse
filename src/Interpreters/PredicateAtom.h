#pragma once

#include <base/types.h>


namespace DB
{

struct PredicateAtom
{
    String column_name;
    String predicate_class;   /// "Equality", "Range", "In", "LikeSubstring", "IsNull", "Other"
    String function_name;     /// "equals", "less", ...
};

}
