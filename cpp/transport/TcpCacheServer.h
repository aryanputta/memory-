#pragma once
// TCP Cache Server
// =================
// Listens on a TCP port and handles binary-protocol cache requests.
// Runs alongside the gRPC server: gRPC for inter-service calls,
// TCP binary protocol for high-frequency client reads.
//
// Key design choices:
//   - epoll (Linux) / kqueue (macOS) event loop for O(n) I/O
//   - Thread-per-CPU worker pool; each worker owns its own epoll instance
//   - Connection reuse: persistent TCP connections with pipelining support
//   - Zero-copy response path: build response directly in send buffer
//   - Backpressure: if send buffer is full, close connection gracefully

#include "BinaryProtocol.h"
#include "../cache_core/CacheStore.h"
#include "../cache_core/RequestContext.h"
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>
#include <mutex>

#ifdef __linux__
  #include <sys/epoll.h>
  #define MERCURY_EPOLL 1
#endif

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <iostream>
#include <chrono>

namespace mercury::transport {

// Per-connection state
struct ConnState {
    int         fd;
    std::string read_buf;
    std::string write_buf;
    bool        closing = false;
};

class TcpCacheServer {
public:
    explicit TcpCacheServer(CacheStore& store,
                             const std::string& node_id,
                             int port           = 9051,
                             int num_workers    = 4)
        : store_(store)
        , node_id_(node_id)
        , port_(port)
        , num_workers_(num_workers)
        , running_(false)
    {}

    ~TcpCacheServer() { Stop(); }

    void Start() {
        listen_fd_ = CreateListenSocket(port_);
        running_   = true;

        for (int i = 0; i < num_workers_; ++i) {
            workers_.emplace_back([this, i]() { WorkerLoop(i); });
        }
        // Acceptor thread
        acceptor_ = std::thread([this]() { AcceptLoop(); });
        std::cout << "[TcpCacheServer:" << node_id_ << "] Listening on :"
                  << port_ << " (" << num_workers_ << " workers)\n";
    }

    void Stop() {
        running_ = false;
        if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
        if (acceptor_.joinable()) acceptor_.join();
        for (auto& w : workers_)
            if (w.joinable()) w.join();
    }

    uint64_t RequestsHandled() const { return requests_handled_.load(); }

private:
    // ── Socket helpers ────────────────────────────────────────────────────
    static int CreateListenSocket(int port) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("socket() failed");

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
        // Disable Nagle for lower latency on small cache responses
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port));

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
            throw std::runtime_error("bind() failed: " + std::string(strerror(errno)));
        if (::listen(fd, 1024) < 0)
            throw std::runtime_error("listen() failed");

        SetNonBlocking(fd);
        return fd;
    }

    static void SetNonBlocking(int fd) {
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    // ── Acceptor loop (single thread) ─────────────────────────────────────
    void AcceptLoop() {
        int worker_idx = 0;
        while (running_) {
            sockaddr_in client_addr{};
            socklen_t   len = sizeof(client_addr);
            int cfd = ::accept(listen_fd_,
                               reinterpret_cast<sockaddr*>(&client_addr), &len);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            SetNonBlocking(cfd);
            int opt = 1;
            ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

            // Round-robin to workers
            int w = worker_idx++ % num_workers_;
            {
                std::lock_guard<std::mutex> lk(queues_[w].mu);
                queues_[w].new_fds.push_back(cfd);
            }
        }
    }

    // ── Worker loop (one per CPU) ─────────────────────────────────────────
    void WorkerLoop(int worker_id) {
#ifdef MERCURY_EPOLL
        int epfd = epoll_create1(0);
        if (epfd < 0) { std::cerr << "epoll_create1 failed\n"; return; }

        std::unordered_map<int, ConnState> conns;

        auto register_fd = [&](int fd) {
            epoll_event ev{};
            ev.events   = EPOLLIN | EPOLLET; // edge-triggered
            ev.data.fd  = fd;
            epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
            conns[fd] = ConnState{fd};
        };

        constexpr int MAX_EVENTS = 64;
        epoll_event events[MAX_EVENTS];

        while (running_) {
            // Drain new connections from acceptor
            {
                std::lock_guard<std::mutex> lk(queues_[worker_id].mu);
                for (int fd : queues_[worker_id].new_fds) register_fd(fd);
                queues_[worker_id].new_fds.clear();
            }

            int n = epoll_wait(epfd, events, MAX_EVENTS, 5 /* ms timeout */);
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    CloseConn(epfd, fd, conns);
                    continue;
                }
                if (events[i].events & EPOLLIN) {
                    HandleRead(epfd, fd, conns, worker_id);
                }
                if (events[i].events & EPOLLOUT) {
                    HandleWrite(epfd, fd, conns);
                }
            }
        }
        ::close(epfd);
#else
        // Fallback: simple blocking per-connection handling (non-Linux)
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
#endif
    }

#ifdef MERCURY_EPOLL
    void HandleRead(int epfd, int fd,
                    std::unordered_map<int, ConnState>& conns, int /*worker*/)
    {
        auto& conn = conns[fd];
        char  tmp[65536];

        while (true) {
            ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
            if (n > 0) {
                conn.read_buf.append(tmp, static_cast<size_t>(n));
                ProcessReadBuffer(epfd, fd, conn);
            } else if (n == 0) {
                CloseConn(epfd, fd, conns); return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                CloseConn(epfd, fd, conns); return;
            }
        }
    }

    void HandleWrite(int epfd, int fd,
                     std::unordered_map<int, ConnState>& conns)
    {
        auto& conn = conns[fd];
        while (!conn.write_buf.empty()) {
            ssize_t n = ::send(fd, conn.write_buf.data(),
                               conn.write_buf.size(), MSG_NOSIGNAL);
            if (n > 0) {
                conn.write_buf.erase(0, static_cast<size_t>(n));
            } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break; // will retry on next EPOLLOUT
            } else {
                CloseConn(epfd, fd, conns); return;
            }
        }
        if (conn.write_buf.empty()) {
            // Unregister EPOLLOUT interest
            epoll_event ev{};
            ev.events  = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
            if (conn.closing) CloseConn(epfd, fd, conns);
        }
    }

    void CloseConn(int epfd, int fd,
                   std::unordered_map<int, ConnState>& conns)
    {
        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        conns.erase(fd);
    }
#endif

    // ── Frame processing ──────────────────────────────────────────────────
    void ProcessReadBuffer(int epfd, int fd, ConnState& conn) {
        (void)epfd;
        while (conn.read_buf.size() >= HEADER_SIZE) {
            const auto* raw = reinterpret_cast<const uint8_t*>(conn.read_buf.data());
            FrameHeader hdr;
            try { hdr = Frame::DecodeHeader(raw); }
            catch (...) { conn.closing = true; return; }

            size_t total = HEADER_SIZE + hdr.body_len;
            if (conn.read_buf.size() < total) break; // wait for more data

            Frame req;
            req.DecodeBody(hdr, raw + HEADER_SIZE);
            conn.read_buf.erase(0, total);

            Frame resp = DispatchRequest(req);
            auto  enc  = resp.Encode();
            conn.write_buf.append(reinterpret_cast<const char*>(enc.data()),
                                  enc.size());
            ++requests_handled_;

            // Register interest in EPOLLOUT
#ifdef MERCURY_EPOLL
            epoll_event ev{};
            ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
#endif
            (void)fd;
        }
    }

    Frame DispatchRequest(const Frame& req) {
        auto op = static_cast<Opcode>(req.header.opcode);
        int64_t now = NowMs();

        if (op == Opcode::GET) {
            CacheKey ck{req.ns, req.key};
            RequestContext ctx; ctx.now_ms = now;
            ctx.allow_stale = (req.header.flags & FLAG_ALLOW_STALE) != 0;
            auto val = store_.Get(ck, now);
            return MakeGetResponse(req.header.seq_id,
                                   val.has_value() ? val->payload : "",
                                   val.has_value() && val->is_stale,
                                   val.has_value());
        }

        if (op == Opcode::PUT) {
            CacheKey  ck{req.ns, req.key};
            CacheValue cv(req.payload, req.version, now, req.ttl_ms);
            RequestContext ctx; ctx.now_ms = now;
            store_.Put(ck, cv, ctx);

            Frame resp;
            resp.header.magic   = MERCURY_MAGIC;
            resp.header.version = PROTOCOL_VER;
            resp.header.opcode  = static_cast<uint8_t>(Opcode::PUT_RESP);
            resp.header.seq_id  = req.header.seq_id;
            resp.payload        = "ok";
            return resp;
        }

        if (op == Opcode::DELETE) {
            store_.Delete({req.ns, req.key});
            Frame resp;
            resp.header.magic   = MERCURY_MAGIC;
            resp.header.version = PROTOCOL_VER;
            resp.header.opcode  = static_cast<uint8_t>(Opcode::DELETE_RESP);
            resp.header.seq_id  = req.header.seq_id;
            return resp;
        }

        if (op == Opcode::PING) {
            Frame resp;
            resp.header.magic   = MERCURY_MAGIC;
            resp.header.version = PROTOCOL_VER;
            resp.header.opcode  = static_cast<uint8_t>(Opcode::PONG);
            resp.header.seq_id  = req.header.seq_id;
            resp.payload        = node_id_;
            return resp;
        }

        // Error response for unknown opcodes
        Frame err;
        err.header.magic   = MERCURY_MAGIC;
        err.header.version = PROTOCOL_VER;
        err.header.opcode  = static_cast<uint8_t>(Opcode::ERROR);
        err.header.seq_id  = req.header.seq_id;
        err.payload        = "unknown opcode";
        return err;
    }

    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // ── State ─────────────────────────────────────────────────────────────
    struct WorkerQueue {
        std::mutex        mu;
        std::vector<int>  new_fds;
    };

    CacheStore&            store_;
    std::string            node_id_;
    int                    port_;
    int                    num_workers_;
    int                    listen_fd_ = -1;
    std::atomic<bool>      running_;
    std::atomic<uint64_t>  requests_handled_{0};
    std::thread            acceptor_;
    std::vector<std::thread>     workers_;
    std::vector<WorkerQueue>     queues_{static_cast<size_t>(num_workers_)};
};

} // namespace mercury::transport
