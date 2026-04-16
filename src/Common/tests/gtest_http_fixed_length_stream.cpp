#include <gtest/gtest.h>

#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/NetException.h>

#include <thread>
#include <atomic>


namespace
{

/// A sink server that accepts one connection and reads everything until closed.
class SinkServer
{
public:
    SinkServer() : server_socket(Poco::Net::SocketAddress("127.0.0.1", 0))
    {
    }

    ~SinkServer()
    {
        done.store(true);
        if (thread.joinable())
            thread.join();
    }

    void start()
    {
        thread = std::thread([this]
        {
            try
            {
                auto conn = server_socket.acceptConnection();
                char buf[4096];
                while (!done.load())
                {
                    try
                    {
                        conn.setReceiveTimeout(Poco::Timespan(0, 100'000));
                        int n = conn.receiveBytes(buf, sizeof(buf));
                        if (n <= 0)
                            break;
                    }
                    catch (const Poco::TimeoutException &) {} // NOLINT
                }
            }
            catch (...) {} // NOLINT
        });
    }

    Poco::UInt16 port() const { return server_socket.address().port(); }

private:
    Poco::Net::ServerSocket server_socket;
    std::thread thread;
    std::atomic<bool> done{false};
};

}


Poco::Net::StreamSocket connectTo(const SinkServer & server)
{
    Poco::Net::StreamSocket sock;
    sock.connect(Poco::Net::SocketAddress("127.0.0.1", server.port()));
    return sock;
}


/// Writing exactly Content-Length bytes should succeed.
TEST(HTTPFixedLengthStreamBuf, WriteExactLength)
{
    SinkServer server;
    server.start();

    auto sock = connectTo(server);
    Poco::Net::HTTPClientSession session(sock);

    Poco::Net::HTTPFixedLengthOutputStream stream(session, 10);

    stream.write("0123456789", 10);
    stream.flush();

    ASSERT_TRUE(stream.good()) << "Stream should be good after writing exactly Content-Length bytes";
}


/// Writing more than Content-Length should throw MessageException.
TEST(HTTPFixedLengthStreamBuf, WriteOverLengthThrows)
{
    SinkServer server;
    server.start();

    auto sock = connectTo(server);
    Poco::Net::HTTPClientSession session(sock);

    /// Content-Length is 5, but we will try to write 10 bytes.
    Poco::Net::HTTPFixedLengthOutputStream stream(session, 5);

    /// The data goes into the 8KB buffer first. On flush, flushBuffer calls
    /// writeToDevice which clamps to Content-Length (writes 5 bytes), then the
    /// loop calls writeToDevice again with the remaining 5 bytes, which throws
    /// MessageException because _count >= _length.
    stream.write("0123456789", 10);

    bool got_exception = false;
    try
    {
        stream.flush();
    }
    catch (const Poco::Net::MessageException &)
    {
        got_exception = true;
    }

    ASSERT_TRUE(got_exception) << "Expected MessageException when writing past Content-Length";
}


/// Writing exactly Content-Length and then one more byte should throw.
TEST(HTTPFixedLengthStreamBuf, WriteBoundaryPlusOneThrows)
{
    SinkServer server;
    server.start();

    auto sock = connectTo(server);
    Poco::Net::HTTPClientSession session(sock);

    Poco::Net::HTTPFixedLengthOutputStream stream(session, 5);

    /// Write exactly 5 + 1 bytes.
    stream.write("012345", 6);

    bool got_exception = false;
    try
    {
        stream.flush();
    }
    catch (const Poco::Net::MessageException &)
    {
        got_exception = true;
    }

    ASSERT_TRUE(got_exception) << "Expected MessageException when writing Content-Length + 1";
}
