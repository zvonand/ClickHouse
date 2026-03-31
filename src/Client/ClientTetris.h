#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>

namespace DB
{

class ClientBase;

/// Minimal terminal Tetris overlay while a query runs (easter egg). Toggled with `t`.
class ClientTetris
{
public:
    explicit ClientTetris(ClientBase & client_);
    ~ClientTetris();

    void toggle();
    void handleKey(char c);
    /// Called when the keystroke interceptor stops (query finished or cancelled).
    void onInterceptorStop();

    bool isActive() const { return active.load(); }

private:
    void runLoop();
    void tickGravity();
    void renderLocked();
    void clearOverlayLocked();
    void resetGameLocked();
    void spawnPieceLocked();
    void mergePieceLocked();
    void clearLinesLocked();
    void hardDropLocked();
    bool collidesLocked(int px, int py, int rot, int type) const;

    ClientBase & client;

    std::atomic<bool> active{false};
    std::thread loop_thread;
    std::mutex game_mutex;

    static constexpr int board_w = 10;
    static constexpr int board_h = 20;

    int8_t board[board_h][board_w]{};
    int piece_type = 0;
    int rotation = 0;
    int piece_x = 0;
    int piece_y = 0;
    bool game_over = false;
    uint64_t rng = 1;
};

}
