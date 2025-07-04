#pragma once

#include <Disks/DiskType.h>

#include <expected>
#include <memory>
#include <optional>
#include <vector>
#include <base/types.h>

#include <Common/LoggingFormatStringHelpers.h>

namespace DB
{
class IStoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const IStoragePolicy>;
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
using Volumes = std::vector<VolumePtr>;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using Disks = std::vector<DiskPtr>;
class IReservation;
using ReservationSharedPtr = std::shared_ptr<IReservation>;
using ReservationPtr = std::unique_ptr<IReservation>;
struct ReservationError
{
    int code;
    PreformattedMessage message;

    template <typename... Args>
    ReservationError(int code_, FormatStringHelper<Args...> formatter, Args &&... args)
        : code(code_)
        , message(formatter.format(std::forward<Args>(args)...))
    {
    }
};
using ReservationPtrOrError = std::expected<ReservationPtr, ReservationError>;
using Reservations = std::vector<ReservationPtr>;

using String = std::string;

class IStoragePolicy
{
public:
    virtual ~IStoragePolicy() = default;
    virtual const String & getName() const = 0;
    virtual const Volumes & getVolumes() const = 0;
    /// Returns number [0., 1.] -- fraction of free space on disk
    /// which should be kept with help of background moves
    virtual double getMoveFactor() const = 0;
    virtual bool isDefaultPolicy() const = 0;
    /// Returns disks ordered by volumes priority
    virtual Disks getDisks() const = 0;
    /// Returns any disk
    /// Used when it's not important, for example for
    /// mutations files
    virtual DiskPtr getAnyDisk() const = 0;
    virtual DiskPtr tryGetDiskByName(const String & disk_name) const = 0;
    DiskPtr getDiskByName(const String & disk_name) const;
    /// Get free space from most free disk
    virtual UInt64 getMaxUnreservedFreeSpace() const = 0;
    /// Reserves space on any volume with index > min_volume_index or returns error
    virtual ReservationPtrOrError reserve(UInt64 bytes, size_t min_volume_index) const = 0;
    /// Returns valid reservation or error
    virtual ReservationPtrOrError reserve(UInt64 bytes) const = 0;
    /// Reserves space on any volume or throws
    virtual ReservationPtr reserveAndCheck(UInt64 bytes) const = 0;
    /// Reserves 0 bytes on disk with max available space
    /// Do not use this function when it is possible to predict size.
    virtual ReservationPtr makeEmptyReservationOnLargestDisk() const = 0;
    /// Get volume by index.
    virtual VolumePtr getVolume(size_t index) const = 0;
    virtual VolumePtr tryGetVolumeByName(const String & volume_name) const = 0;
    VolumePtr getVolumeByName(const String & volume_name) const;
    /// Checks if storage policy can be replaced by another one.
    virtual void checkCompatibleWith(const StoragePolicyPtr & new_storage_policy) const = 0;
    /// If the policy allows table partition operations (move, replace) with the other storage policy.
    virtual bool isCompatibleForPartitionOps(const StoragePolicyPtr & other) const = 0;
    /// Finds a volume index, which contains disk
    virtual std::optional<size_t> tryGetVolumeIndexByDiskName(const String & disk_name) const = 0;
    size_t getVolumeIndexByDiskName(const String & disk_name) const;
    /// Finds a volume which contains a specified disk.
    VolumePtr tryGetVolumeByDiskName(const String & disk_name) const;
    VolumePtr getVolumeByDiskName(const String & disk_name) const;
    /// Check if we have any volume with stopped merges
    virtual bool hasAnyVolumeWithDisabledMerges() const = 0;
    virtual bool containsVolume(const String & volume_name) const = 0;
    /// Returns disks by type ordered by volumes priority
};

}
