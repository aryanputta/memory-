#pragma once
// MercuryCache++ Binary Wire Protocol
// =====================================
// A simple, fixed-header binary framing protocol for cache node communication.
//
// WHY TCP + CUSTOM BINARY PROTOCOL (not just gRPC)?
//   gRPC over HTTP/2 carries significant framing overhead (~30–50 bytes per
//   request + TLS handshake) and requires HTTP/2 multiplexing state machine.
//   For the hot read path (sub-100µs target latency), a purpose-built TCP
//   binary protocol over a persistent connection pool eliminates that overhead.
//
//   This mirrors what production cache systems do:
//   - Memcached uses a custom binary/text protocol over TCP
//   - Redis uses RESP (REdis Serialization Protocol) over TCP
//   - Amazon DAX uses a proprietary binary protocol over TCP
//
// FRAME FORMAT (fixed 16-byte header + variable body):
//
//   ┌────────────────────────────────────────────────────────────┐
//   │  magic   │ version │ opcode │ flags │ seq_id  │ body_len  │
//   │  2 bytes │ 1 byte  │ 1 byte │1 byte │ 4 bytes │  4 bytes  │
//   │──────────────────────────────────────────────────────────  │
//   │  ns_len  │ key_len │        padding (2 bytes)              │
//   │  1 byte  │ 1 byte  │                                       │
//   ├────────────────────────────────────────────────────────────┤
//   │  namespace (ns_len bytes)                                  │
//   │  key       (key_len bytes)                                 │
//   │  payload   (body_len - ns_len - key_len bytes)             │
//   └────────────────────────────────────────────────────────────┘
//
// Total header = 16 bytes.  All multi-byte fields are big-endian (network order).

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>

#ifdef _WIN32
  #include <winsock2.h>
#else
  #include <arpa/inet.h>
#endif

namespace mercury::transport {

// ── Constants ────────────────────────────────────────────────────────────────
static constexpr uint16_t MERCURY_MAGIC   = 0x4D43; // 'MC'
static constexpr uint8_t  PROTOCOL_VER    = 1;
static constexpr size_t   HEADER_SIZE     = 16;
static constexpr size_t   MAX_BODY_SIZE   = 64 * 1024 * 1024; // 64 MB

// ── Opcodes ──────────────────────────────────────────────────────────────────
enum class Opcode : uint8_t {
    GET        = 0x01,
    GET_RESP   = 0x02,
    PUT        = 0x03,
    PUT_RESP   = 0x04,
    DELETE     = 0x05,
    DELETE_RESP= 0x06,
    PING       = 0x07,
    PONG       = 0x08,
    BATCH_GET  = 0x09,
    BATCH_RESP = 0x0A,
    REPLICATE  = 0x0B,
    INVALIDATE = 0x0C,
    ERROR      = 0xFF,
};

// ── Flags (bitmask) ──────────────────────────────────────────────────────────
static constexpr uint8_t FLAG_STALE       = 0x01; // response: value is stale
static constexpr uint8_t FLAG_NOT_FOUND   = 0x02; // response: key not found
static constexpr uint8_t FLAG_ALLOW_STALE = 0x04; // request: accept stale
static constexpr uint8_t FLAG_TTL_SET     = 0x08; // request: TTL field present

// ── Frame header (packed, network-byte-order on wire) ────────────────────────
#pragma pack(push, 1)
struct FrameHeader {
    uint16_t magic;     // MERCURY_MAGIC
    uint8_t  version;   // PROTOCOL_VER
    uint8_t  opcode;    // Opcode enum value
    uint8_t  flags;     // FLAG_* bitmask
    uint32_t seq_id;    // monotonic request ID (for pipelining)
    uint32_t body_len;  // total bytes after header
    uint8_t  ns_len;    // length of namespace string within body
    uint8_t  key_len;   // length of key string within body
    uint8_t  _pad[2];   // reserved

    // Serialise to network byte order in-place
    void ToNetwork() {
        magic    = htons(magic);
        seq_id   = htonl(seq_id);
        body_len = htonl(body_len);
    }

    // Deserialise from network byte order in-place
    void FromNetwork() {
        magic    = ntohs(magic);
        seq_id   = ntohl(seq_id);
        body_len = ntohl(body_len);
    }
};
#pragma pack(pop)

static_assert(sizeof(FrameHeader) == HEADER_SIZE,
    "FrameHeader must be exactly 16 bytes");

// ── Higher-level Frame ───────────────────────────────────────────────────────
struct Frame {
    FrameHeader header;
    std::string ns;
    std::string key;
    std::string payload;  // value or error message
    int64_t     ttl_ms  = 0;
    int64_t     version = 0;

    // Encode the frame to a byte buffer ready for send()
    std::vector<uint8_t> Encode() const {
        std::vector<uint8_t> buf;

        // Build body: ns + key + payload (+ optional 8-byte TTL + 8-byte version)
        bool has_ttl = (header.flags & FLAG_TTL_SET) != 0;
        size_t body_len = ns.size() + key.size() + payload.size()
                        + (has_ttl ? 16 : 0); // ttl_ms(8) + version(8)

        buf.resize(HEADER_SIZE + body_len);
        auto* hdr = reinterpret_cast<FrameHeader*>(buf.data());

        *hdr = header;
        hdr->ns_len   = static_cast<uint8_t>(ns.size());
        hdr->key_len  = static_cast<uint8_t>(key.size());
        hdr->body_len = static_cast<uint32_t>(body_len);
        hdr->ToNetwork();

        uint8_t* pos = buf.data() + HEADER_SIZE;
        std::memcpy(pos, ns.data(),      ns.size());      pos += ns.size();
        std::memcpy(pos, key.data(),     key.size());     pos += key.size();
        if (has_ttl) {
            int64_t net_ttl  = static_cast<int64_t>(htonl(static_cast<uint32_t>(ttl_ms)));
            int64_t net_ver  = static_cast<int64_t>(htonl(static_cast<uint32_t>(version)));
            std::memcpy(pos, &net_ttl, 8); pos += 8;
            std::memcpy(pos, &net_ver, 8); pos += 8;
        }
        std::memcpy(pos, payload.data(), payload.size());

        return buf;
    }

    // Decode a header from raw bytes (exactly HEADER_SIZE bytes)
    static FrameHeader DecodeHeader(const uint8_t* data) {
        FrameHeader hdr;
        std::memcpy(&hdr, data, HEADER_SIZE);
        hdr.FromNetwork();
        if (hdr.magic != MERCURY_MAGIC)
            throw std::runtime_error("Invalid Mercury frame magic");
        if (hdr.version != PROTOCOL_VER)
            throw std::runtime_error("Unsupported protocol version");
        if (hdr.body_len > MAX_BODY_SIZE)
            throw std::runtime_error("Frame body too large");
        return hdr;
    }

    // Decode body bytes into ns/key/payload fields
    void DecodeBody(const FrameHeader& hdr, const uint8_t* body) {
        header = hdr;
        ns.assign(reinterpret_cast<const char*>(body), hdr.ns_len);
        body += hdr.ns_len;
        key.assign(reinterpret_cast<const char*>(body), hdr.key_len);
        body += hdr.key_len;

        bool has_ttl = (hdr.flags & FLAG_TTL_SET) != 0;
        if (has_ttl) {
            std::memcpy(&ttl_ms,  body, 8); body += 8;
            std::memcpy(&version, body, 8); body += 8;
            ttl_ms  = static_cast<int64_t>(ntohl(static_cast<uint32_t>(ttl_ms)));
            version = static_cast<int64_t>(ntohl(static_cast<uint32_t>(version)));
        }

        size_t payload_len = hdr.body_len - hdr.ns_len - hdr.key_len
                           - (has_ttl ? 16 : 0);
        payload.assign(reinterpret_cast<const char*>(body), payload_len);
    }
};

// ── Request/response builders ────────────────────────────────────────────────
inline Frame MakeGetRequest(const std::string& ns, const std::string& key,
                             uint32_t seq_id, bool allow_stale = false)
{
    Frame f;
    f.header.magic   = MERCURY_MAGIC;
    f.header.version = PROTOCOL_VER;
    f.header.opcode  = static_cast<uint8_t>(Opcode::GET);
    f.header.flags   = allow_stale ? FLAG_ALLOW_STALE : 0;
    f.header.seq_id  = seq_id;
    f.ns  = ns;
    f.key = key;
    return f;
}

inline Frame MakePutRequest(const std::string& ns, const std::string& key,
                             const std::string& value, int64_t ttl_ms,
                             int64_t version, uint32_t seq_id)
{
    Frame f;
    f.header.magic   = MERCURY_MAGIC;
    f.header.version = PROTOCOL_VER;
    f.header.opcode  = static_cast<uint8_t>(Opcode::PUT);
    f.header.flags   = FLAG_TTL_SET;
    f.header.seq_id  = seq_id;
    f.ns      = ns;
    f.key     = key;
    f.payload = value;
    f.ttl_ms  = ttl_ms;
    f.version = version;
    return f;
}

inline Frame MakeGetResponse(uint32_t seq_id, const std::string& value,
                              bool stale, bool found)
{
    Frame f;
    f.header.magic   = MERCURY_MAGIC;
    f.header.version = PROTOCOL_VER;
    f.header.opcode  = static_cast<uint8_t>(Opcode::GET_RESP);
    f.header.flags   = (stale ? FLAG_STALE : 0) | (found ? 0 : FLAG_NOT_FOUND);
    f.header.seq_id  = seq_id;
    f.payload = found ? value : "";
    return f;
}

} // namespace mercury::transport
