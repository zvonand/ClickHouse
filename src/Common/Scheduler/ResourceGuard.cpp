#include <Common/Scheduler/ResourceGuard.h>
#include <Common/CurrentThread.h>

namespace DB
{

void ResourceGuard::Request::wait()
{
    CurrentMetrics::Increment scheduled(metrics->scheduled_count);
    auto timer = CurrentThread::getProfileEvents().timer(metrics->wait_microseconds);
    std::unique_lock lock(mutex);
    dequeued_cv.wait(lock, [this] { return state == Dequeued; });
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
}

}
