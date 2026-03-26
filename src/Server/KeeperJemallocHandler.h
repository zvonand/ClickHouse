#pragma once

#include "config.h"

#if USE_NURAFT

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Serves the static jemalloc.html page for Keeper
class KeeperJemallocWebUIHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

/// Returns jemalloc profile/stats/status data via REST API for Keeper.
/// Routes:
///   GET /jemalloc/profile?format={collapsed|raw}  — heap profile
///   GET /jemalloc/stats                           — malloc_stats_print output
///   GET /jemalloc/status                          — profiling state as JSON
///
/// When jemalloc is not linked (sanitizer builds), routes return HTTP 501.
class KeeperJemallocAPIHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
#if USE_JEMALLOC
    void handleProfile(HTTPServerRequest & request, HTTPServerResponse & response);
    void handleStats(HTTPServerRequest & request, HTTPServerResponse & response);
    void handleStatus(HTTPServerRequest & request, HTTPServerResponse & response);
#endif
};

}

#endif
