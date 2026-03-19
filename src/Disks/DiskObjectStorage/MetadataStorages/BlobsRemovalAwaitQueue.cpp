#include <Disks/DiskObjectStorage/MetadataStorages/BlobsRemovalAwaitQueue.h>
#include <Common/UniqueLock.h>

#include <condition_variable>
#include <limits>
#include <ranges>

namespace DB
{

struct BlobsRemovalAwaitQueue::Waiter
{
    size_t remaining = 0;
    std::condition_variable cv = {};
};

void BlobsRemovalAwaitQueue::waitRemoval(const StoredObjects & blobs, std::unique_lock<std::mutex> & lock)
{
    Waiter waiter{blobs.size()};
    for (const auto & blob : blobs)
        blob_to_waiters[blob].push_back(&waiter);

    waiter.cv.wait(lock, [&waiter]() { return waiter.remaining == 0; });
}

void BlobsRemovalAwaitQueue::notifyRemoval(const StoredObject & blob, std::unique_lock<std::mutex> &)
{
    auto it = blob_to_waiters.find(blob);
    if (it == blob_to_waiters.end())
        return;

    for (auto * waiter : it->second)
    {
        waiter->remaining -= 1;
        if (waiter->remaining == 0)
            waiter->cv.notify_one();
    }

    blob_to_waiters.erase(it);
}

void BlobsRemovalAwaitQueue::addBlobsPendingRemoval(const StoredObjects & blobs, bool need_wait_removal)
{
    std::unique_lock lock(mutex);

    objects_to_remove.insert_range(blobs);

    if (need_wait_removal)
        waitRemoval(blobs, lock);
}

IMetadataStorage::BlobsToRemove BlobsRemovalAwaitQueue::getBlobsPendingRemoval(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::unique_lock lock(mutex);

    if (max_count == 0)
        max_count = std::numeric_limits<int64_t>::max();

    IMetadataStorage::BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove | std::views::take(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t BlobsRemovalAwaitQueue::recordAsRemoved(const StoredObjects & blobs)
{
    std::unique_lock lock(mutex);

    int64_t recorded_count = 0;
    for (const auto & removed_blob : blobs)
        recorded_count += objects_to_remove.erase(removed_blob);

    for (const auto & blob : blobs)
        notifyRemoval(blob, lock);

    return recorded_count;
}

}
