#pragma once
#include <string>
#include <cstdint>

namespace mercury {

struct CacheValue {
    std::string payload;       // serialized content (JSON, protobuf bytes, etc.)
    int64_t     version;       // monotonic version / etag
    int64_t     created_at_ms; // wall-clock ms when inserted
    int64_t     expires_at_ms; // 0 means no expiry
    bool        is_stale;      // set when soft-TTL has passed but hard-TTL has not

    CacheValue() = default;

    CacheValue(std::string payload_, int64_t version_,
               int64_t created_at_ms_, int64_t ttl_ms)
        : payload(std::move(payload_))
        , version(version_)
        , created_at_ms(created_at_ms_)
        , expires_at_ms(ttl_ms > 0 ? created_at_ms_ + ttl_ms : 0)
        , is_stale(false)
    {}

    bool IsExpired(int64_t now_ms) const {
        return expires_at_ms > 0 && now_ms >= expires_at_ms;
    }

    // Soft-expire: entry is "stale" but still serveable
    bool IsSoftExpired(int64_t now_ms, int64_t soft_ttl_ms) const {
        if (expires_at_ms == 0) return false;
        int64_t soft_expire = expires_at_ms - soft_ttl_ms;
        return now_ms >= soft_expire;
    }

    int64_t TtlRemainingMs(int64_t now_ms) const {
        if (expires_at_ms == 0) return -1; // never expires
        int64_t remaining = expires_at_ms - now_ms;
        return remaining > 0 ? remaining : 0;
    }
};

} // namespace mercury
