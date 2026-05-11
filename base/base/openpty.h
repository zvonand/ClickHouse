#pragma once

#include <fcntl.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

#include <cstdlib>


/// Portable replacement for libc's `openpty` using POSIX `posix_openpt`/`grantpt`/`unlockpt`.
/// Opens a new pseudo-terminal pair, applies the window size to the slave side,
/// and writes the master/slave file descriptors into the output references.
/// Returns 0 on success, -1 on failure with `errno` set.
inline int openPty(int & master_fd, int & slave_fd, const winsize & ws)
{
    int m = posix_openpt(O_RDWR | O_NOCTTY);
    if (m < 0)
        return -1;

    if (grantpt(m) != 0 || unlockpt(m) != 0)
    {
        close(m);
        return -1;
    }

    /// On Linux, `ptsname` is not thread-safe, so we use `ptsname_r`.
    /// On macOS and FreeBSD, `ptsname` uses thread-local storage and is thread-safe;
    /// the `ptsname_r` variant is not available there.
    char slave_name[64];

#if defined(OS_LINUX)
    if (ptsname_r(m, slave_name, sizeof(slave_name)) != 0)
    {
        close(m);
        return -1;
    }
#else
    const char * name = ptsname(m);
    if (name == nullptr)
    {
        close(m);
        return -1;
    }
    /// Bounded copy of the slave path into a local buffer.
    size_t i = 0;
    while (name[i] != '\0' && i + 1 < sizeof(slave_name))
    {
        slave_name[i] = name[i];
        ++i;
    }
    slave_name[i] = '\0';
#endif

    int s = open(slave_name, O_RDWR | O_NOCTTY);
    if (s < 0)
    {
        close(m);
        return -1;
    }

    if (ioctl(s, TIOCSWINSZ, &ws) != 0)
    {
        close(s);
        close(m);
        return -1;
    }

    master_fd = m;
    slave_fd = s;

    return 0;
}
