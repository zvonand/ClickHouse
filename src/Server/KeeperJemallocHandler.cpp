#include <Server/KeeperJemallocHandler.h>

#if USE_NURAFT

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>

#if USE_JEMALLOC
#    include <Common/Jemalloc.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/ReadHelpers.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Stringifier.h>
#    include <base/scope_guard.h>
#    include <filesystem>
#endif

/// Reuse the server's jemalloc HTML — the page auto-adapts via window.JEMALLOC_CONFIG.
constexpr unsigned char resource_jemalloc_html[] =
{
#embed "../../programs/server/jemalloc.html"
};

namespace DB
{

void KeeperJemallocWebUIHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string html(reinterpret_cast<const char *>(resource_jemalloc_html), std::size(resource_jemalloc_html));

    constexpr std::string_view head_close = "<head>";
    auto pos = html.find(head_close);
    if (pos != std::string::npos)
        html.insert(pos + head_close.size(), "<script>window.JEMALLOC_CONFIG={mode:'keeper'}</script>");

    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(html.data(), html.size());
    wb.finalize();
}

void KeeperJemallocAPIHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
try
{
    Poco::URI uri(request.getURI());
    const std::string & path = uri.getPath();

    if (path == "/jemalloc/")
    {
        setResponseDefaultHeaders(response);
        response.redirect("/jemalloc", Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY);
        return;
    }

#if USE_JEMALLOC
    if (path == "/jemalloc/profile")
        handleProfile(request, response);
    else if (path == "/jemalloc/stats")
        handleStats(request, response);
    else if (path == "/jemalloc/status")
        handleStatus(request, response);
    else
    {
        setResponseDefaultHeaders(response);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        response.setContentType("text/plain");
        *response.send() << "Not found\n";
    }
#else
    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED);
    response.setContentType("text/plain");
    *response.send() << "jemalloc profiling is not available in this build\n";
#endif
}
catch (...)
{
    tryLogCurrentException("KeeperJemallocAPIHandler");

    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            *response.send() << getCurrentExceptionMessage(false) << '\n';
    }
    catch (...)
    {
        LOG_ERROR(getLogger("KeeperJemallocAPIHandler"), "Cannot send exception to client");
    }
}

#if USE_JEMALLOC

void KeeperJemallocAPIHandler::handleProfile(
    HTTPServerRequest & request, HTTPServerResponse & response)
{
    Poco::URI uri(request.getURI());
    auto params = uri.getQueryParameters();

    std::string format = "collapsed";
    for (const auto & [key, value] : params)
    {
        if (key == "format")
            format = value;
    }

    if (format != "collapsed" && format != "raw")
    {
        setResponseDefaultHeaders(response);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        response.setContentType("text/plain");
        *response.send() << "Unknown format: " << format << ". Supported: collapsed, raw\n";
        return;
    }

    if (request.getMethod() == HTTPRequest::HTTP_HEAD)
    {
        setResponseDefaultHeaders(response);
        response.setContentType("text/plain; charset=UTF-8");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        response.send();
        return;
    }

    auto raw_file = std::string(Jemalloc::flushProfile("/tmp/jemalloc_keeper"));
    SCOPE_EXIT({ std::error_code ec; std::filesystem::remove(raw_file, ec); });

    std::string output;

    if (format == "collapsed")
    {
        output = Jemalloc::heapProfileToCollapsedStacks(raw_file);
    }
    else
    {
        ReadBufferFromFile in(raw_file);
        readStringUntilEOF(output, in);
    }

    setResponseDefaultHeaders(response);
    response.setContentType("text/plain; charset=UTF-8");
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(output.data(), output.size());
    wb.finalize();
}

void KeeperJemallocAPIHandler::handleStats(
    HTTPServerRequest & request, HTTPServerResponse & response)
{
    auto stats = Jemalloc::getStats();

    setResponseDefaultHeaders(response);
    response.setContentType("text/plain; charset=UTF-8");
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(stats.data(), stats.size());
    wb.finalize();
}

void KeeperJemallocAPIHandler::handleStatus(
    HTTPServerRequest & request, HTTPServerResponse & response)
{
    Poco::JSON::Object json;
    Poco::JSON::Array errors;

    auto readMallctl = [&]<typename T>(const char * name, T default_value) -> T
    {
        try
        {
            return Jemalloc::getValue<T>(name);
        }
        catch (...)
        {
            tryLogCurrentException("KeeperJemallocAPIHandler", std::string("Failed to read mallctl '") + name + "'");
            errors.add(std::string(name));
            return default_value;
        }
    };

    bool prof_enabled = readMallctl("opt.prof", false);
    bool prof_active = false;
    bool thread_active_init = false;
    size_t lg_sample = 0;

    if (prof_enabled)
    {
        prof_active = readMallctl("prof.active", false);
        lg_sample = readMallctl("prof.lg_sample", size_t(0));
        try
        {
            thread_active_init = Jemalloc::getThreadProfileInitMib().getValue();
        }
        catch (...)
        {
            tryLogCurrentException("KeeperJemallocAPIHandler", "Failed to read prof.thread_active_init");
            errors.add("prof.thread_active_init");
        }
    }

    json.set("prof_enabled", prof_enabled);
    json.set("prof_active", prof_active);
    json.set("thread_active_init", thread_active_init);
    json.set("lg_sample", static_cast<Poco::UInt64>(lg_sample));
    if (errors.size() > 0)
        json.set("errors", errors);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);

    setResponseDefaultHeaders(response);
    response.setContentType("application/json");
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    auto str = oss.str();
    wb.write(str.data(), str.size());
    wb.finalize();
}

#endif

}
#endif
