#include <Access/Common/JWKSProvider.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/StreamCopier.h>
#include <fstream>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int INVALID_CONFIG_PARAMETER;
}

jwt::jwks<jwt::traits::kazuho_picojson> JWKSClient::getJWKS()
{
    std::shared_lock lock(mutex);

    auto now = std::chrono::high_resolution_clock::now();
    auto diff = std::chrono::duration<double, std::milli>(now - last_request_send).count();

    if (diff < refresh_ms) {
        jwt::jwks <jwt::traits::kazuho_picojson> result(cached_jwks);
        return result;
    }

    Poco::Net::HTTPResponse response;
    std::ostringstream responseString;

    Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, jwks_uri.getPathAndQuery()};

    if (jwks_uri.getScheme() == "https") {
        Poco::Net::HTTPSClientSession session = Poco::Net::HTTPSClientSession(jwks_uri.getHost(), jwks_uri.getPort());
        session.sendRequest(request);
        std::istream & responseStream = session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK || !responseStream)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to get user info by access token, code: {}, reason: {}", response.getStatus(), response.getReason());
        Poco::StreamCopier::copyStream(responseStream, responseString);
    } else {
        Poco::Net::HTTPClientSession session = Poco::Net::HTTPClientSession(jwks_uri.getHost(), jwks_uri.getPort());
        session.sendRequest(request);
        std::istream & responseStream = session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK || !responseStream)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to get user info by access token, code: {}, reason: {}", response.getStatus(), response.getReason());
        Poco::StreamCopier::copyStream(responseStream, responseString);
    }

    last_request_send = std::chrono::high_resolution_clock::now();

    jwt::jwks<jwt::traits::kazuho_picojson> parsed_jwks;

    try {
        parsed_jwks = jwt::parse_jwks(responseString.str());
    }
    catch (const Exception & e) {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to parse JWKS: {}", e.what());
    }

    cached_jwks = std::move(parsed_jwks);
    return cached_jwks;
}

StaticJWKSParams::StaticJWKSParams(const std::string &static_jwks_, const std::string &static_jwks_file_)
{
    if (static_jwks_.empty() && static_jwks_file_.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "JWT validator misconfigured: `static_jwks` or `static_jwks_file` keys must be present in static JWKS validator configuration");
    if (!static_jwks_.empty() && !static_jwks_file_.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "JWT validator misconfigured: `static_jwks` and `static_jwks_file` keys cannot both be present in static JWKS validator configuration");

    static_jwks = static_jwks_;
    static_jwks_file = static_jwks_file_;
}

StaticJWKS::StaticJWKS(const StaticJWKSParams &params)
{
    String content = String(params.static_jwks);
    if (!params.static_jwks_file.empty()) {
        std::ifstream ifs(params.static_jwks_file);
        content = String((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    }
    auto keys = jwt::parse_jwks(content);
    jwks = std::move(keys);
}

}
