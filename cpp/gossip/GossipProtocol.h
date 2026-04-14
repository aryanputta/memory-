#pragma once
// SWIM Gossip Protocol — UDP-based Cluster Membership & Failure Detection
// =========================================================================
// Reference: "SWIM: Scalable Weakly-consistent Infection-style Process Group
//             Membership Protocol" — Das, Gupta & Motivala, DSN 2002
//
// WHY UDP FOR GOSSIP (not TCP)?
//   TCP requires a 3-way handshake per connection — expensive when pinging
//   N nodes every few seconds. UDP is fire-and-forget with <1ms overhead.
//   Membership messages are small (< 512 bytes) and packet loss is
//   handled by protocol retries, not TCP retransmission.
//
//   This is how production systems do it:
//   - Amazon Cassandra: uses modified SWIM over UDP
//   - HashiCorp Consul/Serf: memberlist library, SWIM over UDP
//   - Kubernetes: kubelet gossips node status via UDP heartbeats
//
// SWIM PROTOCOL MECHANICS:
//
//   Direct probe (PING):
//     Every T_probe ms, pick a random member and send PING via UDP.
//     If no ACK within T_ack ms → suspect the member.
//
//   Indirect probe (PING-REQ):
//     Ask k random other members to PING the suspect on our behalf.
//     If they get an ACK they forward it back.
//     If still no ACK within T_indirect ms → mark member DEAD.
//
//   Dissemination (piggyback):
//     Membership updates (join, suspect, dead) are piggybacked on every
//     outgoing message (infection-style). Each update propagates to all
//     N members in O(log N) rounds.
//
// MESSAGE FORMAT (all fields big-endian):
//
//   ┌──────────────────────────────────────────────────────┐
//   │ magic(2) │ version(1) │ msg_type(1) │ sender_len(1) │
//   │ incarnation(4) │ payload_len(2)                      │
//   │ sender   (sender_len bytes)                          │
//   │ payload  (payload_len bytes) — piggybacked updates   │
//   └──────────────────────────────────────────────────────┘

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <chrono>
#include <random>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>

namespace mercury::gossip {

// ── Constants ────────────────────────────────────────────────────────────────
static constexpr uint16_t GOSSIP_MAGIC   = 0x5357; // 'SW' (SWIM)
static constexpr uint8_t  GOSSIP_VERSION = 1;
static constexpr int      GOSSIP_PORT    = 7946;    // same port as Consul/Serf
static constexpr int      T_PROBE_MS     = 1000;    // probe interval
static constexpr int      T_ACK_MS       = 300;     // ack timeout
static constexpr int      T_INDIRECT_MS  = 600;     // indirect probe timeout
static constexpr int      K_INDIRECT     = 3;       // indirect probe fanout
static constexpr int      MAX_UDP_PACKET = 1400;    // safe below MTU

// ── Message types ────────────────────────────────────────────────────────────
enum class MsgType : uint8_t {
    PING        = 0x01,  // direct probe
    ACK         = 0x02,  // probe acknowledged
    PING_REQ    = 0x03,  // ask someone else to probe on my behalf
    NACK        = 0x04,  // indirect probe failed
    JOIN        = 0x05,  // new member announcement
    SUSPECT     = 0x06,  // member may be down
    DEAD        = 0x07,  // member confirmed dead
    ALIVE       = 0x08,  // member confirmed alive (overrides suspect)
    SYNC        = 0x09,  // full membership list sync (on join)
};

// ── Member state ─────────────────────────────────────────────────────────────
enum class MemberStatus { Alive, Suspect, Dead };

struct Member {
    std::string   address;     // "host:port" e.g. "10.0.0.1:7946"
    MemberStatus  status      = MemberStatus::Alive;
    uint32_t      incarnation = 0; // incremented when we override a suspect
    int64_t       last_seen_ms = 0;
};

// ── Wire message (parsed from UDP datagram) ───────────────────────────────────
struct GossipMessage {
    MsgType     type;
    std::string sender;     // "host:port" of the sender
    uint32_t    incarnation = 0;
    std::string target;     // for PING_REQ: the suspect to probe
    std::string payload;    // piggybacked membership updates (CSV encoded)
};

// ── Encoder/Decoder ───────────────────────────────────────────────────────────
inline std::vector<uint8_t> EncodeMessage(const GossipMessage& msg) {
    std::vector<uint8_t> buf;
    buf.reserve(64);

    auto push16 = [&](uint16_t v) {
        v = htons(v);
        buf.push_back((v >> 8) & 0xFF);
        buf.push_back(v & 0xFF);
    };
    auto push32 = [&](uint32_t v) {
        v = htonl(v);
        for (int i = 3; i >= 0; --i) buf.push_back((v >> (i*8)) & 0xFF);
    };
    auto pushStr = [&](const std::string& s) {
        buf.push_back(static_cast<uint8_t>(std::min<size_t>(s.size(), 255)));
        buf.insert(buf.end(), s.begin(),
                   s.begin() + std::min<size_t>(s.size(), 255));
    };

    push16(GOSSIP_MAGIC);
    buf.push_back(GOSSIP_VERSION);
    buf.push_back(static_cast<uint8_t>(msg.type));
    push32(msg.incarnation);
    pushStr(msg.sender);
    pushStr(msg.target);
    // payload length (2 bytes) + payload
    uint16_t plen = static_cast<uint16_t>(
        std::min<size_t>(msg.payload.size(), 65535));
    push16(plen);
    buf.insert(buf.end(), msg.payload.begin(),
               msg.payload.begin() + plen);
    return buf;
}

inline bool DecodeMessage(const uint8_t* data, size_t len, GossipMessage& out) {
    if (len < 12) return false;
    size_t pos = 0;
    auto read16 = [&]() -> uint16_t {
        uint16_t v = (static_cast<uint16_t>(data[pos]) << 8) | data[pos+1];
        pos += 2; return ntohs(v);
    };
    auto read32 = [&]() -> uint32_t {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v = (v << 8) | data[pos++];
        return ntohl(v);
    };
    auto readStr = [&]() -> std::string {
        if (pos >= len) return "";
        uint8_t slen = data[pos++];
        if (pos + slen > len) return "";
        std::string s(reinterpret_cast<const char*>(data + pos), slen);
        pos += slen; return s;
    };

    uint16_t magic = read16();
    if (magic != GOSSIP_MAGIC) return false;
    uint8_t ver = data[pos++];
    if (ver != GOSSIP_VERSION) return false;
    out.type        = static_cast<MsgType>(data[pos++]);
    out.incarnation = read32();
    out.sender      = readStr();
    out.target      = readStr();
    if (pos + 2 > len) return false;
    uint16_t plen   = read16();
    if (pos + plen > len) return false;
    out.payload.assign(reinterpret_cast<const char*>(data + pos), plen);
    return true;
}

// ── SWIM Gossip Node ──────────────────────────────────────────────────────────
class GossipNode {
public:
    using ChangeCallback = std::function<void(const std::string& addr,
                                              MemberStatus status)>;

    GossipNode(const std::string& my_address,
               int gossip_port       = GOSSIP_PORT,
               ChangeCallback on_change = nullptr)
        : my_address_(my_address)
        , gossip_port_(gossip_port)
        , on_change_(std::move(on_change))
        , rng_(std::random_device{}())
        , running_(false)
    {
        // Seed our own entry
        members_[my_address_] = Member{my_address_, MemberStatus::Alive, 0, NowMs()};
    }

    ~GossipNode() { Stop(); }

    void Start() {
        udp_fd_  = CreateUdpSocket(gossip_port_);
        running_ = true;
        recv_thread_  = std::thread([this]() { ReceiveLoop(); });
        probe_thread_ = std::thread([this]() { ProbeLoop(); });
        std::cout << "[Gossip:" << my_address_ << "] Started on UDP :"
                  << gossip_port_ << "\n";
    }

    void Stop() {
        running_ = false;
        if (udp_fd_ >= 0) { ::close(udp_fd_); udp_fd_ = -1; }
        if (recv_thread_.joinable())  recv_thread_.join();
        if (probe_thread_.joinable()) probe_thread_.join();
    }

    // Announce ourselves to a seed node and request full membership list
    void Join(const std::string& seed_address) {
        GossipMessage msg;
        msg.type        = MsgType::JOIN;
        msg.sender      = my_address_;
        msg.incarnation = 0;
        msg.payload     = SerializeMembership();
        SendTo(seed_address, msg);
        std::cout << "[Gossip:" << my_address_ << "] Sent JOIN to " << seed_address << "\n";
    }

    std::vector<Member> LiveMembers() const {
        std::lock_guard<std::mutex> lk(members_mu_);
        std::vector<Member> result;
        for (const auto& [addr, m] : members_)
            if (m.status == MemberStatus::Alive && addr != my_address_)
                result.push_back(m);
        return result;
    }

    int MemberCount() const {
        std::lock_guard<std::mutex> lk(members_mu_);
        return static_cast<int>(members_.size());
    }

    std::string DumpState() const {
        std::lock_guard<std::mutex> lk(members_mu_);
        std::ostringstream oss;
        oss << "Gossip state for " << my_address_ << ":\n";
        for (const auto& [addr, m] : members_) {
            const char* st = m.status == MemberStatus::Alive   ? "ALIVE  " :
                             m.status == MemberStatus::Suspect ? "SUSPECT" : "DEAD   ";
            oss << "  " << st << "  " << addr << "  inc=" << m.incarnation << "\n";
        }
        return oss.str();
    }

private:
    // ── UDP socket ────────────────────────────────────────────────────────
    static int CreateUdpSocket(int port) {
        int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) throw std::runtime_error("UDP socket() failed");

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

        // 1 second recv timeout so ReceiveLoop() can check running_
        struct timeval tv{1, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port));
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
            throw std::runtime_error("UDP bind() failed: " +
                                     std::string(strerror(errno)));
        return fd;
    }

    void SendTo(const std::string& address, const GossipMessage& msg) {
        // address = "host:port"
        auto colon = address.rfind(':');
        if (colon == std::string::npos) return;
        std::string host = address.substr(0, colon);
        int         port = std::stoi(address.substr(colon + 1));

        sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port   = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, host.c_str(), &dest.sin_addr) <= 0) return;

        auto buf = EncodeMessage(msg);
        ::sendto(udp_fd_, buf.data(), buf.size(), 0,
                 reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
    }

    // ── Receive loop ──────────────────────────────────────────────────────
    void ReceiveLoop() {
        uint8_t     buf[MAX_UDP_PACKET];
        sockaddr_in src{};
        socklen_t   src_len = sizeof(src);

        while (running_) {
            ssize_t n = ::recvfrom(udp_fd_, buf, sizeof(buf), 0,
                                   reinterpret_cast<sockaddr*>(&src), &src_len);
            if (n <= 0) continue; // timeout or error

            GossipMessage msg;
            if (!DecodeMessage(buf, static_cast<size_t>(n), msg)) continue;
            HandleMessage(msg);
        }
    }

    void HandleMessage(const GossipMessage& msg) {
        switch (msg.type) {
        case MsgType::PING: {
            // Respond with ACK
            GossipMessage ack;
            ack.type        = MsgType::ACK;
            ack.sender      = my_address_;
            ack.incarnation = GetIncarnation();
            ack.payload     = BuildPiggybackUpdates();
            SendTo(msg.sender, ack);
            MergeUpdates(msg.payload);
            UpdateMember(msg.sender, MemberStatus::Alive, msg.incarnation);
            break;
        }
        case MsgType::ACK: {
            // Record as alive; notify pending probe
            UpdateMember(msg.sender, MemberStatus::Alive, msg.incarnation);
            MergeUpdates(msg.payload);
            {
                std::lock_guard<std::mutex> lk(pending_mu_);
                pending_acks_.insert(msg.sender);
            }
            break;
        }
        case MsgType::PING_REQ: {
            // Probe target on sender's behalf
            GossipMessage probe;
            probe.type        = MsgType::PING;
            probe.sender      = my_address_;
            probe.incarnation = GetIncarnation();
            probe.payload     = BuildPiggybackUpdates();
            SendTo(msg.target, probe);
            // (We could forward the ACK back to original requester)
            break;
        }
        case MsgType::JOIN: {
            // New member: share full membership list
            UpdateMember(msg.sender, MemberStatus::Alive, msg.incarnation);
            MergeUpdates(msg.payload);
            GossipMessage sync;
            sync.type        = MsgType::SYNC;
            sync.sender      = my_address_;
            sync.incarnation = GetIncarnation();
            sync.payload     = SerializeMembership();
            SendTo(msg.sender, sync);
            break;
        }
        case MsgType::SYNC: {
            MergeUpdates(msg.payload);
            UpdateMember(msg.sender, MemberStatus::Alive, msg.incarnation);
            break;
        }
        case MsgType::SUSPECT: {
            MarkSuspect(msg.target, msg.incarnation);
            break;
        }
        case MsgType::DEAD: {
            MarkDead(msg.target, msg.incarnation);
            break;
        }
        case MsgType::ALIVE: {
            UpdateMember(msg.target, MemberStatus::Alive, msg.incarnation);
            break;
        }
        default: break;
        }
    }

    // ── Probe loop (SWIM failure detection) ──────────────────────────────
    void ProbeLoop() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(T_PROBE_MS));

            std::string target = PickRandomMember();
            if (target.empty()) continue;

            // Direct PING
            {
                std::lock_guard<std::mutex> lk(pending_mu_);
                pending_acks_.erase(target);
            }
            GossipMessage ping;
            ping.type        = MsgType::PING;
            ping.sender      = my_address_;
            ping.incarnation = GetIncarnation();
            ping.payload     = BuildPiggybackUpdates();
            SendTo(target, ping);

            std::this_thread::sleep_for(std::chrono::milliseconds(T_ACK_MS));

            bool acked = false;
            {
                std::lock_guard<std::mutex> lk(pending_mu_);
                acked = pending_acks_.count(target) > 0;
            }

            if (!acked) {
                // Indirect probe via K random other members
                auto others = PickRandomMembers(K_INDIRECT, {target});
                for (const auto& relay : others) {
                    GossipMessage preq;
                    preq.type        = MsgType::PING_REQ;
                    preq.sender      = my_address_;
                    preq.target      = target;
                    preq.incarnation = GetIncarnation();
                    SendTo(relay, preq);
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(
                    T_INDIRECT_MS - T_ACK_MS));

                bool indirect_acked = false;
                {
                    std::lock_guard<std::mutex> lk(pending_mu_);
                    indirect_acked = pending_acks_.count(target) > 0;
                }

                if (!indirect_acked) {
                    MarkSuspect(target, 0);
                    // After one more probe cycle without ack → DEAD
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(T_PROBE_MS));
                    {
                        std::lock_guard<std::mutex> lk(pending_mu_);
                        indirect_acked = pending_acks_.count(target) > 0;
                    }
                    if (!indirect_acked) {
                        MarkDead(target, 0);
                        // Gossip DEAD to others
                        auto others2 = PickRandomMembers(3, {target});
                        for (const auto& peer : others2) {
                            GossipMessage dead;
                            dead.type    = MsgType::DEAD;
                            dead.sender  = my_address_;
                            dead.target  = target;
                            SendTo(peer, dead);
                        }
                    }
                }
            }
        }
    }

    // ── Member management ─────────────────────────────────────────────────
    void UpdateMember(const std::string& addr, MemberStatus status,
                      uint32_t incarnation) {
        std::lock_guard<std::mutex> lk(members_mu_);
        auto& m = members_[addr];
        if (incarnation < m.incarnation) return; // stale update
        bool changed = (m.status != status);
        m.address     = addr;
        m.status      = status;
        m.incarnation = incarnation;
        m.last_seen_ms = NowMs();
        if (changed && on_change_) on_change_(addr, status);
    }

    void MarkSuspect(const std::string& addr, uint32_t inc) {
        if (addr == my_address_) {
            // Refute by incrementing our own incarnation
            std::lock_guard<std::mutex> lk(members_mu_);
            members_[my_address_].incarnation++;
            return;
        }
        UpdateMember(addr, MemberStatus::Suspect, inc);
    }

    void MarkDead(const std::string& addr, uint32_t inc) {
        if (addr == my_address_) return; // can't be dead if we're running
        UpdateMember(addr, MemberStatus::Dead, inc);
    }

    uint32_t GetIncarnation() const {
        std::lock_guard<std::mutex> lk(members_mu_);
        auto it = members_.find(my_address_);
        return it != members_.end() ? it->second.incarnation : 0;
    }

    // ── Piggybacking ─────────────────────────────────────────────────────
    // Simple CSV: "addr|status|incarnation;addr|status|incarnation;..."
    std::string SerializeMembership() const {
        std::lock_guard<std::mutex> lk(members_mu_);
        std::ostringstream oss;
        for (const auto& [addr, m] : members_) {
            oss << addr << "|"
                << static_cast<int>(m.status) << "|"
                << m.incarnation << ";";
        }
        return oss.str();
    }

    std::string BuildPiggybackUpdates() {
        // Limit to last 5 updates to stay under MTU
        std::lock_guard<std::mutex> lk(members_mu_);
        std::ostringstream oss;
        int count = 0;
        for (const auto& [addr, m] : members_) {
            if (++count > 5) break;
            oss << addr << "|" << static_cast<int>(m.status)
                << "|" << m.incarnation << ";";
        }
        return oss.str();
    }

    void MergeUpdates(const std::string& payload) {
        if (payload.empty()) return;
        std::istringstream iss(payload);
        std::string token;
        while (std::getline(iss, token, ';')) {
            if (token.empty()) continue;
            auto p1 = token.find('|');
            auto p2 = token.rfind('|');
            if (p1 == std::string::npos || p1 == p2) continue;
            std::string addr = token.substr(0, p1);
            int         st   = std::stoi(token.substr(p1+1, p2-p1-1));
            uint32_t    inc  = static_cast<uint32_t>(
                                   std::stoul(token.substr(p2+1)));
            UpdateMember(addr, static_cast<MemberStatus>(st), inc);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────
    std::string PickRandomMember() {
        std::lock_guard<std::mutex> lk(members_mu_);
        std::vector<std::string> alive;
        for (const auto& [addr, m] : members_)
            if (addr != my_address_ && m.status == MemberStatus::Alive)
                alive.push_back(addr);
        if (alive.empty()) return "";
        std::uniform_int_distribution<size_t> dist(0, alive.size()-1);
        return alive[dist(rng_)];
    }

    std::vector<std::string> PickRandomMembers(
        int k, const std::unordered_set<std::string>& exclude = {})
    {
        std::lock_guard<std::mutex> lk(members_mu_);
        std::vector<std::string> candidates;
        for (const auto& [addr, m] : members_)
            if (addr != my_address_ && m.status == MemberStatus::Alive
                && !exclude.count(addr))
                candidates.push_back(addr);

        std::shuffle(candidates.begin(), candidates.end(), rng_);
        if (static_cast<int>(candidates.size()) > k)
            candidates.resize(static_cast<size_t>(k));
        return candidates;
    }

    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // ── Member state ──────────────────────────────────────────────────────
    std::string                          my_address_;
    int                                  gossip_port_;
    ChangeCallback                       on_change_;
    mutable std::mutex                   members_mu_;
    std::unordered_map<std::string, Member> members_;

    // Pending ACK tracking
    std::mutex                  pending_mu_;
    std::unordered_set<std::string> pending_acks_;

    int                 udp_fd_  = -1;
    std::atomic<bool>   running_;
    std::thread         recv_thread_;
    std::thread         probe_thread_;
    std::mt19937        rng_;
};

} // namespace mercury::gossip
