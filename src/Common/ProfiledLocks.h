#pragma once

#include <Common/SharedMutex.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>

#include <mutex>
#include <shared_mutex>


namespace DB
{

/// RAII lock guard that measures both wait-time and hold-time for std::mutex.
/// Increments the wait-event after the lock is acquired, and the hold-event when the guard is destroyed.
class TSA_SCOPED_LOCKABLE ProfiledMutexLock final
{
public:
    ProfiledMutexLock(std::mutex & mutex, ProfileEvents::Event wait_event, ProfileEvents::Event hold_event_) TSA_ACQUIRE(mutex)
        : hold_event(hold_event_)
    {
        Stopwatch wait_watch;
        std::unique_lock<std::mutex> l(mutex);
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        lock = std::move(l);
        hold_watch.restart();
    }

    ~ProfiledMutexLock() TSA_RELEASE()
    {
        if (lock.owns_lock())
        {
            UInt64 elapsed = hold_watch.elapsedMicroseconds();
            lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
    }

    ProfiledMutexLock(const ProfiledMutexLock &) = delete;
    ProfiledMutexLock & operator=(const ProfiledMutexLock &) = delete;
    ProfiledMutexLock(ProfiledMutexLock &&) = delete;
    ProfiledMutexLock & operator=(ProfiledMutexLock &&) = delete;

private:
    std::unique_lock<std::mutex> lock;
    ProfileEvents::Event hold_event;
    Stopwatch hold_watch;
};

/// RAII lock guard that measures both wait-time and hold-time for exclusive (write) locks on SharedMutex.
/// Increments the wait-event after the lock is acquired, and the hold-event when the guard is destroyed.
class TSA_SCOPED_LOCKABLE ProfiledExclusiveLock final
{
public:
    ProfiledExclusiveLock(SharedMutex & mutex, ProfileEvents::Event wait_event, ProfileEvents::Event hold_event_) TSA_ACQUIRE(mutex)
        : hold_event(hold_event_)
    {
        Stopwatch wait_watch;
        std::unique_lock<SharedMutex> l(mutex);
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        lock = std::move(l);
        hold_watch.restart();
    }

    ~ProfiledExclusiveLock() TSA_RELEASE()
    {
        if (lock.owns_lock())
        {
            UInt64 elapsed = hold_watch.elapsedMicroseconds();
            lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
    }

    ProfiledExclusiveLock(const ProfiledExclusiveLock &) = delete;
    ProfiledExclusiveLock & operator=(const ProfiledExclusiveLock &) = delete;
    ProfiledExclusiveLock(ProfiledExclusiveLock &&) = delete;
    ProfiledExclusiveLock & operator=(ProfiledExclusiveLock &&) = delete;

private:
    std::unique_lock<SharedMutex> lock;
    ProfileEvents::Event hold_event;
    Stopwatch hold_watch;
};


/// RAII lock guard that measures both wait-time and hold-time for shared (read) locks on SharedMutex.
class TSA_SCOPED_LOCKABLE ProfiledSharedLock final
{
public:
    ProfiledSharedLock(SharedMutex & mutex, ProfileEvents::Event wait_event, ProfileEvents::Event hold_event_) TSA_ACQUIRE_SHARED(mutex)
        : hold_event(hold_event_)
    {
        Stopwatch wait_watch;
        std::shared_lock<SharedMutex> l(mutex);
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        lock = std::move(l);
        hold_watch.restart();
    }

    ~ProfiledSharedLock() TSA_RELEASE()
    {
        if (lock.owns_lock())
        {
            UInt64 elapsed = hold_watch.elapsedMicroseconds();
            lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
    }

    ProfiledSharedLock(const ProfiledSharedLock &) = delete;
    ProfiledSharedLock & operator=(const ProfiledSharedLock &) = delete;
    ProfiledSharedLock(ProfiledSharedLock &&) = delete;
    ProfiledSharedLock & operator=(ProfiledSharedLock &&) = delete;

private:
    std::shared_lock<SharedMutex> lock;
    ProfileEvents::Event hold_event;
    Stopwatch hold_watch;
};

}
