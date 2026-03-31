#include <Client/ClientTetris.h>
#include <Client/ClientBase.h>
#include <Common/TerminalSize.h>
#include <base/types.h>

#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <thread>


namespace DB
{

namespace
{

int8_t g_shapes[7][4][4][4];

struct ShapesInit
{
    ShapesInit()
    {
        static const int8_t BASE[7][4][4] = {
            {{0, 0, 0, 0}, {1, 1, 1, 1}, {0, 0, 0, 0}, {0, 0, 0, 0}},
            {{0, 1, 1, 0}, {0, 1, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}},
            {{0, 1, 0, 0}, {1, 1, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}},
            {{0, 1, 1, 0}, {1, 1, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}},
            {{1, 1, 0, 0}, {0, 1, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}},
            {{1, 0, 0, 0}, {1, 1, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}},
            {{0, 0, 1, 0}, {1, 1, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}},
        };

        auto rotate = [](const int8_t in[4][4], int8_t out[4][4])
        {
            for (int y = 0; y < 4; ++y)
                for (int x = 0; x < 4; ++x)
                    out[x][3 - y] = in[y][x];
        };

        int8_t cur[4][4];
        int8_t nxt[4][4];
        for (int p = 0; p < 7; ++p)
        {
            memcpy(cur, BASE[p], sizeof(cur));
            for (int r = 0; r < 4; ++r)
            {
                memcpy(g_shapes[p][r], cur, sizeof(cur));
                rotate(cur, nxt);
                memcpy(cur, nxt, sizeof(cur));
            }
        }
    }
};

const ShapesInit shapes_init;

}

ClientTetris::ClientTetris(ClientBase & client_) : client(client_)
{
}

ClientTetris::~ClientTetris()
{
    onInterceptorStop();
}

void ClientTetris::onInterceptorStop()
{
    active = false;
    if (loop_thread.joinable())
        loop_thread.join();
    std::lock_guard lock(game_mutex);
    clearOverlayLocked();
}

void ClientTetris::toggle()
{
    bool start = false;
    bool stop = false;
    {
        std::lock_guard lock(game_mutex);
        if (!active.load())
        {
            active = true;
            resetGameLocked();
            start = true;
        }
        else
        {
            active = false;
            stop = true;
        }
    }
    if (stop)
    {
        if (loop_thread.joinable())
            loop_thread.join();
        std::lock_guard lock(game_mutex);
        clearOverlayLocked();
        return;
    }
    if (start)
    {
        if (loop_thread.joinable())
            loop_thread.join();
        loop_thread = std::thread([this] { runLoop(); });
        std::lock_guard lock(game_mutex);
        renderLocked();
    }
}

void ClientTetris::runLoop()
{
    while (active.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(450));
        if (!active.load())
            break;
        std::lock_guard lock(game_mutex);
        if (!active.load())
            break;
        if (!game_over)
            tickGravity();
        renderLocked();
    }
}

bool ClientTetris::collidesLocked(int px, int py, int rot, int type) const
{
    const int r = (type == 1) ? 0 : rot;
    for (int y = 0; y < 4; ++y)
    {
        for (int x = 0; x < 4; ++x)
        {
            if (!g_shapes[type][r][y][x])
                continue;
            const int bx = px + x;
            const int by = py + y;
            if (bx < 0 || bx >= board_w || by >= board_h)
                return true;
            if (by >= 0 && board[by][bx])
                return true;
        }
    }
    return false;
}

void ClientTetris::resetGameLocked()
{
    memset(board, 0, sizeof(board));
    game_over = false;
    spawnPieceLocked();
}

void ClientTetris::spawnPieceLocked()
{
    rng = rng * 6364136223846793005ULL + 1;
    piece_type = static_cast<int>((rng >> 32) % 7);
    rotation = 0;
    piece_x = 3;
    piece_y = 0;
    if (collidesLocked(piece_x, piece_y, rotation, piece_type))
        game_over = true;
}

void ClientTetris::mergePieceLocked()
{
    const int r = (piece_type == 1) ? 0 : rotation;
    for (int y = 0; y < 4; ++y)
    {
        for (int x = 0; x < 4; ++x)
        {
            if (!g_shapes[piece_type][r][y][x])
                continue;
            const int bx = piece_x + x;
            const int by = piece_y + y;
            if (by >= 0 && by < board_h && bx >= 0 && bx < board_w)
                board[by][bx] = 1;
        }
    }
}

void ClientTetris::clearLinesLocked()
{
    for (int y = board_h - 1; y >= 0; --y)
    {
        bool full = true;
        for (int x = 0; x < board_w; ++x)
        {
            if (!board[y][x])
            {
                full = false;
                break;
            }
        }
        if (full)
        {
            for (int y2 = y; y2 > 0; --y2)
                memcpy(board[y2], board[y2 - 1], board_w);
            memset(board[0], 0, board_w);
            ++y;
        }
    }
}

void ClientTetris::tickGravity()
{
    if (collidesLocked(piece_x, piece_y + 1, rotation, piece_type))
    {
        mergePieceLocked();
        clearLinesLocked();
        spawnPieceLocked();
    }
    else
        ++piece_y;
}

void ClientTetris::hardDropLocked()
{
    if (game_over)
        return;
    while (!collidesLocked(piece_x, piece_y + 1, rotation, piece_type))
        ++piece_y;
    mergePieceLocked();
    clearLinesLocked();
    spawnPieceLocked();
}

void ClientTetris::renderLocked()
{
    if (!client.tty_buf)
        return;

    String frame;
    frame.reserve(4096);
    fmt::format_to(std::back_inserter(frame), "\033[s\033[1;1H");

    fmt::format_to(std::back_inserter(frame), "Tetris  t:exit  h/l:move  j:down  k:rotate  space:drop  r:restart\r\n");

    if (game_over)
        fmt::format_to(std::back_inserter(frame), "GAME OVER\r\n");

    const int r = (piece_type == 1) ? 0 : rotation;
    for (int row = 0; row < board_h; ++row)
    {
        fmt::format_to(std::back_inserter(frame), "|");
        for (int col = 0; col < board_w; ++col)
        {
            char c = board[row][col] ? '#' : ' ';
            if (!board[row][col])
            {
                for (int py = 0; py < 4; ++py)
                {
                    for (int px = 0; px < 4; ++px)
                    {
                        if (g_shapes[piece_type][r][py][px] && piece_x + px == col && piece_y + py == row)
                        {
                            c = '@';
                            break;
                        }
                    }
                    if (c == '@')
                        break;
                }
            }
            fmt::format_to(std::back_inserter(frame), "{}", c);
        }
        fmt::format_to(std::back_inserter(frame), "|\r\n");
    }
    fmt::format_to(std::back_inserter(frame), "+");
    for (int i = 0; i < board_w; ++i)
        fmt::format_to(std::back_inserter(frame), "-");
    fmt::format_to(std::back_inserter(frame), "+\033[u");

    client.tty_buf->write(frame.data(), frame.size());
    client.tty_buf->next();
}

void ClientTetris::clearOverlayLocked()
{
    if (!client.tty_buf)
        return;

    uint16_t term_w = 80;
    uint16_t term_h = 28;
    try
    {
        const auto sz = getTerminalSize(client.stdin_fd, client.stderr_fd);
        term_w = std::max<uint16_t>(sz.first, 40);
        term_h = std::min<uint16_t>(std::max<uint16_t>(sz.second, 24), 200);
    }
    catch (...)
    {
    }

    const size_t w = term_w;
    const size_t h = term_h;
    String blank(w, ' ');
    String s;
    s.reserve(h * (w + 32));
    fmt::format_to(std::back_inserter(s), "\033[s");
    for (size_t row = 0; row < h; ++row)
        fmt::format_to(std::back_inserter(s), "\033[{};1H\033[K{}", row + 1, blank);
    fmt::format_to(std::back_inserter(s), "\033[u");

    client.tty_buf->write(s.data(), s.size());
    client.tty_buf->next();
}

void ClientTetris::handleKey(char c)
{
    std::lock_guard lock(game_mutex);
    if (!active.load())
        return;

    if (c == 'r' || c == 'R')
    {
        resetGameLocked();
        renderLocked();
        return;
    }

    if (game_over)
        return;

    switch (c)
    {
        case 'h':
            if (!collidesLocked(piece_x - 1, piece_y, rotation, piece_type))
                --piece_x;
            break;
        case 'l':
            if (!collidesLocked(piece_x + 1, piece_y, rotation, piece_type))
                ++piece_x;
            break;
        case 'j':
            if (!collidesLocked(piece_x, piece_y + 1, rotation, piece_type))
                ++piece_y;
            else
            {
                mergePieceLocked();
                clearLinesLocked();
                spawnPieceLocked();
            }
            break;
        case 'k':
        {
            const int nr = (rotation + 1) % 4;
            if (!collidesLocked(piece_x, piece_y, nr, piece_type))
                rotation = nr;
            break;
        }
        case ' ':
            hardDropLocked();
            break;
        default:
            break;
    }
    renderLocked();
}

}
