#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>

#include <mutex>

namespace DB
{

class BlobsRemovalAwaitQueue
{
    struct Waiter;
    void waitRemoval(const StoredObjects & blobs, std::unique_lock<std::mutex> & lock);
    void notifyRemoval(const StoredObject & blob, std::unique_lock<std::mutex> & lock);

public:
    void addBlobsPendingRemoval(const StoredObjects & blobs, bool need_wait_removal);
    IMetadataStorage::BlobsToRemove getBlobsPendingRemoval(const ClusterConfigurationPtr & cluster, int64_t max_count);
    int64_t recordAsRemoved(const StoredObjects & blobs);

private:
    std::mutex mutex;
    StoredObjectSet objects_to_remove;
    std::unordered_map<StoredObject, std::vector<Waiter *>> blob_to_waiters;
};

}
