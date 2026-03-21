#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>

namespace DB
{

void InMemoryRemovalQueue::submitForRemoval(const StoredObjects & objects)
{
    std::lock_guard guard(mutex);
    for (const auto & obj : objects)
    {
        int64_t slot = next_slot++;
        queue.emplace(slot, obj);
        index.emplace(obj, slot);
    }
}

StoredObjects InMemoryRemovalQueue::takeFirst(int64_t max_count) const
{
    std::lock_guard guard(mutex);

    StoredObjects result;
    for (const auto & [slot, blob] : queue)
    {
        if (max_count > 0 && std::ssize(result) >= max_count)
            break;

        result.push_back(blob);
    }

    return result;
}

int64_t InMemoryRemovalQueue::markAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(mutex);

    int64_t count = 0;
    for (const auto & blob : blobs)
    {
        if (auto it = index.find(blob); it != index.end())
        {
            queue.erase(it->second);
            index.erase(it);
            ++count;
        }
    }

    return count;
}

bool InMemoryRemovalQueue::containsAny(const StoredObjects & blobs) const
{
    std::lock_guard guard(mutex);

    for (const auto & blob : blobs)
        if (index.contains(blob))
            return true;

    return false;
}

}
