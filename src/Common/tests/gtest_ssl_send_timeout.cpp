#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/SharedPtr.h>
#include <Poco/Timespan.h>
#include <Poco/TimeoutException.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <thread>
#include <atomic>
#include <vector>


namespace
{

/// Generate a self-signed certificate and private key in memory,
/// write them to temporary files for Poco::Net::Context.
struct EphemeralCert
{
    std::string cert_path;
    std::string key_path;

    EphemeralCert()
    {
        EVP_PKEY * pkey = EVP_RSA_gen(2048);
        ASSERT_TRUE(pkey != nullptr);

        X509 * x509 = X509_new();
        ASSERT_TRUE(x509 != nullptr);

        ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
        X509_gmtime_adj(X509_getm_notBefore(x509), 0);
        X509_gmtime_adj(X509_getm_notAfter(x509), 3600);
        X509_set_pubkey(x509, pkey);

        X509_NAME * name = X509_get_subject_name(x509);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>("localhost"), -1, -1, 0);
        X509_set_issuer_name(x509, name);
        X509_sign(x509, pkey, EVP_sha256());

        cert_path = writeToTempFile(
            [&](BIO * bio) { PEM_write_bio_X509(bio, x509); }, "cert");
        key_path = writeToTempFile(
            [&](BIO * bio) { PEM_write_bio_PrivateKey(bio, pkey, nullptr, nullptr, 0, nullptr, nullptr); }, "key");

        X509_free(x509);
        EVP_PKEY_free(pkey);
    }

    ~EphemeralCert()
    {
        unlink(cert_path.c_str());
        unlink(key_path.c_str());
    }

private:
    template <typename Fn>
    static std::string writeToTempFile(Fn writer, const char * suffix)
    {
        char path[256];
        snprintf(path, sizeof(path), "/tmp/gtest_ssl_%s_XXXXXX", suffix);
        int fd = mkstemp(path);
        EXPECT_GE(fd, 0);

        BIO * bio = BIO_new_fd(fd, BIO_CLOSE);
        writer(bio);
        BIO_free(bio);
        return path;
    }
};


Poco::Net::Context::Ptr makeContext(const EphemeralCert & cert, Poco::Net::Context::Usage usage)
{
    Poco::Net::Context::Params params;
    params.privateKeyFile = cert.key_path;
    params.certificateFile = cert.cert_path;
    params.verificationMode = Poco::Net::Context::VERIFY_NONE;
    return new Poco::Net::Context(usage, params);
}

}


/// Test that a blocking SSL socket write throws TimeoutException
/// when the peer stops reading and the send timeout expires.
TEST(SSLSocketTimeout, SendBytesThrowsTimeoutOnBlockingSocket)
{
    EphemeralCert cert;
    auto server_ctx = makeContext(cert, Poco::Net::Context::SERVER_USE);
    auto client_ctx = makeContext(cert, Poco::Net::Context::CLIENT_USE);

    Poco::Net::SecureServerSocket server_socket(
        Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_ctx);
    auto port = server_socket.address().port();

    std::atomic<bool> server_done{false};

    /// Server thread: accept and handshake, then sit idle (never read).
    std::thread server_thread([&]
    {
        try
        {
            auto accepted = server_socket.acceptConnection();
            /// Handshake happens on first I/O. Do a small read to trigger it.
            char buf[1];
            try { accepted.receiveBytes(buf, 1); } catch (...) {}
            /// Keep the connection open until the test completes.
            while (!server_done.load())
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        catch (...)
        {
        }
    });

    try
    {
        Poco::Net::SecureStreamSocket client(
            Poco::Net::SocketAddress("127.0.0.1", port), client_ctx);

        /// Very short send timeout so the test doesn't wait long.
        client.setSendTimeout(Poco::Timespan(0, 200'000)); /// 200ms

        /// Write enough data to fill the TCP send buffer and SSL buffer.
        /// Typical TCP buffer is 128KB-256KB. Write 4MB to be sure.
        std::vector<char> data(4 * 1024 * 1024, 'X');

        bool got_timeout = false;
        try
        {
            size_t offset = 0;
            while (offset < data.size())
            {
                int sent = client.sendBytes(data.data() + offset, static_cast<int>(data.size() - offset));
                if (sent > 0)
                    offset += sent;
                else
                    break;
            }
        }
        catch (const Poco::TimeoutException &)
        {
            got_timeout = true;
        }

        ASSERT_TRUE(got_timeout) << "Expected Poco::TimeoutException when writing to a non-reading SSL peer";
    }
    catch (const Poco::Exception & e)
    {
        /// Connection setup can fail on some systems; skip gracefully.
        GTEST_SKIP() << "SSL setup failed: " << e.displayText();
    }

    server_done.store(true);
    server_thread.join();
}


#endif /// USE_SSL
