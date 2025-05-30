#include <Common/Crypto/OpenSSLInitializer.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>

#if USE_SSL
#    include <openssl/crypto.h>
#    include <openssl/provider.h>
#    include <openssl/ssl.h>
#endif


namespace DB
{

#if USE_SSL
std::atomic<uint8_t> DB::OpenSSLInitializer::ref_count{0};
OSSL_PROVIDER * DB::OpenSSLInitializer::default_provider = nullptr;
OSSL_PROVIDER * DB::OpenSSLInitializer::legacy_provider = nullptr;
#endif

OpenSSLInitializer::OpenSSLInitializer()
{
    initialize();
}

void OpenSSLInitializer::initialize()
{
#if USE_SSL
    if (ref_count++ == 0)
    {
        // Disable OpenSSL atexit hook.
        // It may cause issues on shutdown, when some OpenSSL objects are still in use.
        auto openssl_flags = OPENSSL_INIT_NO_ATEXIT;

        /// Loading OpenSSL config only if it is set explicitly.
        ///
        /// 'legacy' provider may be disabled in system config,
        /// but we're compiling it statically in the binary, so it must be loadable in any case.
        const char * env_openssl_conf = getenv("OPENSSL_CONF"); // NOLINT(concurrency-mt-unsafe)
        if (env_openssl_conf)
        {
            openssl_flags |= OPENSSL_INIT_LOAD_CONFIG;
        }
        else
        {
            openssl_flags |= OPENSSL_INIT_NO_LOAD_CONFIG;
        }

        if (OPENSSL_init_ssl(openssl_flags, nullptr) == 0)
            throw std::runtime_error(fmt::format("Failed to load OpenSSL config. {}", getOpenSSLErrors()));

        default_provider = OSSL_PROVIDER_load(nullptr, "default");
        if (!default_provider)
            throw std::runtime_error(fmt::format("Failed to load OpenSSL 'default' provider. {}", getOpenSSLErrors()));

        legacy_provider = OSSL_PROVIDER_load(nullptr, "legacy");
        if (!legacy_provider)
            throw std::runtime_error(fmt::format("Failed to load OpenSSL 'legacy' provider. {}", getOpenSSLErrors()));
    }
#endif
}

OpenSSLInitializer::~OpenSSLInitializer()
{
#if USE_SSL
    if (--ref_count == 0)
    {
        if (legacy_provider)
        {
            chassert(OSSL_PROVIDER_unload(legacy_provider));
            legacy_provider = nullptr;
        }

        if (default_provider)
        {
            chassert(OSSL_PROVIDER_unload(default_provider));
            default_provider = nullptr;
        }
    }
#endif
}

}
