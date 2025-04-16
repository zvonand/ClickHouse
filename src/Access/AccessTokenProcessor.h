#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <Access/Credentials.h>
#include <Common/Exception.h>
#include <Access/JWTValidator.h>
#include <Common/re2.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int INVALID_CONFIG_PARAMETER;
}

class GoogleAccessTokenProcessor;

class IAccessTokenProcessor
{
public:
    IAccessTokenProcessor(const String & name_,
                          const UInt64 cache_invalidation_interval_,
                          const String & email_regex_str)
                          : name(name_),
                            cache_invalidation_interval(cache_invalidation_interval_),
                            email_regex(email_regex_str)
    {
        if (!email_regex_str.empty())
        {
            /// Later, we will use .ok() to determine whether there was a regex specified in config or not.
            if (!email_regex.ok())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid regex in definition of access token processor {}", name);
        }
    }

    virtual ~IAccessTokenProcessor() = default;

    String getName() { return name; }
    UInt64 getCacheInvalidationInterval() { return cache_invalidation_interval; }

    virtual bool resolveAndValidate(const TokenCredentials & credentials) = 0;

    static std::unique_ptr<DB::IAccessTokenProcessor> parseTokenProcessor(
        const Poco::Util::AbstractConfiguration & config,
        const String & prefix,
        const String & name);

protected:
    const String name;
    const UInt64 cache_invalidation_interval;
    re2::RE2 email_regex;
};


class GoogleAccessTokenProcessor : public IAccessTokenProcessor
{
public:
    GoogleAccessTokenProcessor(const String & name_,
                               const UInt64 cache_invalidation_interval_,
                               const String & email_regex_str)
                               : IAccessTokenProcessor(name_, cache_invalidation_interval_, email_regex_str) {}

    bool resolveAndValidate(const TokenCredentials & credentials) override;

private:
    [[maybe_unused]] static const Poco::URI token_info_uri;
    static const Poco::URI user_info_uri;

    std::unordered_map<String, String> getUserInfo(const String & token) const;
};


class AzureAccessTokenProcessor : public IAccessTokenProcessor
{
public:
    AzureAccessTokenProcessor(const String & name_,
                              const UInt64 cache_invalidation_interval_,
                              const String & email_regex_str,
                              const String & tenant_id_)
                              : IAccessTokenProcessor(name_, cache_invalidation_interval_, email_regex_str),
                                jwks_uri_str("https://login.microsoftonline.com/" + tenant_id_ + "/discovery/v2.0/keys") {}

    bool resolveAndValidate(const TokenCredentials & credentials) override;
private:
    static const Poco::URI user_info_uri;

    const String jwks_uri_str;

    String validateTokenAndGetUsername(const String & token) const;
};

}
