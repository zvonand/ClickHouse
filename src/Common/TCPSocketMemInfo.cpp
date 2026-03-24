#include <Common/TCPSocketMemInfo.h>

#if defined(OS_LINUX)

#include <linux/sock_diag.h>
#include <linux/inet_diag.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

namespace DB
{

namespace
{

/// Send a SOCK_DIAG_BY_FAMILY dump request for all ESTABLISHED/CLOSE_WAIT TCP sockets.
bool sendDiagRequest(int nl_fd, uint8_t family)
{
    struct
    {
        nlmsghdr nlh;
        inet_diag_req_v2 req;
    } request{};

    request.nlh.nlmsg_len = sizeof(request);
    request.nlh.nlmsg_type = SOCK_DIAG_BY_FAMILY;
    request.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
    request.req.sdiag_family = family;
    request.req.sdiag_protocol = IPPROTO_TCP;
    request.req.idiag_states = (1 << TCP_ESTABLISHED) | (1 << TCP_CLOSE_WAIT);
    request.req.idiag_ext |= (1 << (INET_DIAG_MEMINFO - 1));

    return send(nl_fd, &request, sizeof(request), 0) == static_cast<ssize_t>(sizeof(request));
}

/// Receive and parse all netlink responses, extracting inode -> TCPSocketMemInfo.
void recvDiagResponse(int nl_fd, std::unordered_map<uint64_t, TCPSocketMemInfo> & result)
{
    char buf[32768]; // NOLINT(modernize-avoid-c-arrays)

    while (true)
    {
        ssize_t len = recv(nl_fd, buf, sizeof(buf), 0);
        if (len <= 0)
            break;

        for (auto * nlh = reinterpret_cast<nlmsghdr *>(buf);
             NLMSG_OK(nlh, static_cast<size_t>(len));
             nlh = NLMSG_NEXT(nlh, len))
        {
            if (nlh->nlmsg_type == NLMSG_DONE)
                return;
            if (nlh->nlmsg_type == NLMSG_ERROR)
                return;

            auto * diag_msg = static_cast<inet_diag_msg *>(NLMSG_DATA(nlh));
            uint64_t inode = diag_msg->idiag_inode;

            unsigned int attr_len = nlh->nlmsg_len - NLMSG_LENGTH(sizeof(*diag_msg));
            for (auto * attr = reinterpret_cast<rtattr *>(diag_msg + 1);
                 RTA_OK(attr, attr_len);
                 attr = RTA_NEXT(attr, attr_len))
            {
                if (attr->rta_type == INET_DIAG_MEMINFO)
                {
                    auto * mem = static_cast<inet_diag_meminfo *>(RTA_DATA(attr));
                    result[inode] = {.rmem = mem->idiag_rmem, .wmem = mem->idiag_tmem};
                }
            }
        }
    }
}

}

std::unordered_map<uint64_t, TCPSocketMemInfo> getTCPSocketMemInfoByInode()
{
    std::unordered_map<uint64_t, TCPSocketMemInfo> result;

    int nl_fd = socket(AF_NETLINK, SOCK_DGRAM | SOCK_CLOEXEC, NETLINK_SOCK_DIAG);
    if (nl_fd < 0)
        return result;

    SCOPE_EXIT({ close(nl_fd); });

    /// Bind to the kernel
    sockaddr_nl addr{};
    addr.nl_family = AF_NETLINK;
    if (bind(nl_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
        return result;

    /// Query IPv4 sockets
    if (sendDiagRequest(nl_fd, AF_INET))
        recvDiagResponse(nl_fd, result);

    /// Query IPv6 sockets
    if (sendDiagRequest(nl_fd, AF_INET6))
        recvDiagResponse(nl_fd, result);

    return result;
}

}

#else

namespace DB
{

std::unordered_map<uint64_t, TCPSocketMemInfo> getTCPSocketMemInfoByInode()
{
    return {};
}

}

#endif
