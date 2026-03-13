#pragma once

#include <atomic>
#include <memory>

#include <Common/Exception.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_UNAVAILABLE_SHARDS;
}

/// Tracks the number of unavailable shards that were skipped during distributed query execution.
/// Shared across all RemoteQueryExecutor instances for a single query.
/// If the number or ratio of skipped shards exceeds the configured thresholds, throws an exception.
struct UnavailableShardTracker
{
    std::atomic<size_t> unavailable_count{0};
    size_t total_shards;
    size_t max_unavailable_num;
    Float64 max_unavailable_ratio;

    UnavailableShardTracker(size_t total_shards_, size_t max_num_, Float64 max_ratio_)
        : total_shards(total_shards_)
        , max_unavailable_num(max_num_)
        , max_unavailable_ratio(max_ratio_)
    {
    }

    /// Called when a shard is determined to be unavailable and would be skipped.
    /// Throws if the configured thresholds are exceeded.
    void onShardSkipped()
    {
        size_t count = ++unavailable_count;

        if (max_unavailable_num > 0 && count > max_unavailable_num)
            throw Exception(
                ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
                "Too many unavailable shards: {} out of {} total shards are unavailable, "
                "max_skip_unavailable_shards_num is set to {}",
                count, total_shards, max_unavailable_num);

        if (max_unavailable_ratio > 0 && total_shards > 0
            && static_cast<Float64>(count) / static_cast<Float64>(total_shards) > max_unavailable_ratio)
            throw Exception(
                ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
                "Too many unavailable shards: {} out of {} total shards are unavailable ({:.1f}%), "
                "max_skip_unavailable_shards_ratio is set to {}",
                count, total_shards, 100.0 * static_cast<double>(count) / static_cast<double>(total_shards), max_unavailable_ratio);
    }
};

using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

}
