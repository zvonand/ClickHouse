#include <gtest/gtest.h>

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Common/Exception.h>

using namespace DB;

namespace
{
constexpr auto FORMAT_VERSION = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
}

/// Two parts can both be marked "uncovered" (no other part contains either of them) and
/// still intersect, because containment and intersection are independent relations:
///
///   `all_0_5_2`  covers blocks 0..5  level 2
///   `all_2_11_3` covers blocks 2..11 level 3
///
/// Neither contains the other (the min/max block ranges are not nested), but they share
/// blocks 2..5. This is the exact failure mode reproduced by the BuzzHouse fuzzer in
/// `StorageReplicatedMergeTree::checkPartsImpl` when both parts pass the
/// `uncovered == true` filter and are then passed to `ActiveDataPartSet::add` (STID
/// `2352-49be`). Use `tryAdd` to return `HasIntersectingPart` without throwing.
TEST(ActiveDataPartSet, IntersectingPartsWithoutContainmentTryAddReturnsOutcome)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_0_5_2"));

    String reason;
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::HasIntersectingPart, set.tryAdd("all_2_11_3", &reason));
    EXPECT_FALSE(reason.empty());

    /// The set still contains the originally-added part and nothing else.
    EXPECT_EQ(1u, set.size());
    EXPECT_EQ("all_0_5_2", set.getContainingPart("all_0_5_1"));
}

/// Symmetric direction: insert the larger range first, then the smaller intersecting one.
/// Confirms the intersection check fires when scanning to the left of the lower bound
/// in `addImpl`.
TEST(ActiveDataPartSet, IntersectingPartsWithoutContainmentTryAddReverseOrder)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_2_11_3"));

    String reason;
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::HasIntersectingPart, set.tryAdd("all_0_5_2", &reason));
    EXPECT_FALSE(reason.empty());
    EXPECT_EQ(1u, set.size());
}

TEST(ActiveDataPartSet, ContainedPartReturnsHasCovering)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_0_11_3"));
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::HasCovering, set.tryAdd("all_2_5_1"));
}

TEST(ActiveDataPartSet, DisjointPartsBothAdd)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_0_5_2"));
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_6_11_3"));
    EXPECT_EQ(2u, set.size());
}
