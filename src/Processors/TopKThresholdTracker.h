#pragma once
#include <shared_mutex>
#include <Core/Field.h>
#include <Common/SharedMutex.h>

class Collator;

namespace DB
{

struct TopKThresholdTracker
{
    TopKThresholdTracker(int direction_, int nulls_direction_, std::shared_ptr<Collator> collator_ = nullptr)
        : direction(direction_), nulls_direction(nulls_direction_), collator(std::move(collator_))
    {
    }

    void testAndSet(const Field & value)
    {
        std::unique_lock lock(mutex);
        if (!is_set)
        {
            threshold = value;
            is_set = true;
            return;
        }
        int cmp = compareFields(value, threshold);
        if (direction == 1 && cmp < 0)
            threshold = value;
        else if (direction == -1 && cmp > 0)
            threshold = value;
    }

    bool isValueInsideThreshold(const Field & value) const
    {
        if (!is_set)
            return true;

        std::shared_lock lock(mutex);
        int cmp = compareFields(value, threshold);
        if (direction == 1 && cmp >= 0)
            return false;
        if (direction == -1 && cmp <= 0)
            return false;

        return true;
    }

    Field getValue() const
    {
        std::shared_lock lock(mutex);
        auto ret = threshold;
        return ret;
    }

    bool isSet() const { return is_set; }

    int getDirection() const { return direction; }
    int getNullsDirection() const { return nulls_direction; }
    const std::shared_ptr<Collator> & getCollator() const { return collator; }

private:
    /// Compare two Field values using the same semantics as the ORDER BY clause:
    /// NULL ordering follows nulls_direction, string comparison uses collator if set.
    int compareFields(const Field & lhs, const Field & rhs) const;

    Field threshold;
    mutable SharedMutex mutex;
    std::atomic<bool> is_set{false};
    int direction{0};
    int nulls_direction{1};
    std::shared_ptr<Collator> collator;
};

using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

}
