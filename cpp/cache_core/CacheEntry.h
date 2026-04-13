#pragma once
#include "CacheKey.h"
#include "CacheValue.h"
#include <cstdint>

namespace mercury {

struct CacheEntry {
    CacheKey   cache_key;
    CacheValue value;
    uint64_t   freq;             // access frequency counter (for LFU)
    int64_t    last_access_ms;   // wall-clock ms of last access
    bool       resident;         // true while the entry lives in the store
    uint64_t   cost_bytes;       // estimated memory footprint

    CacheEntry() : freq(0), last_access_ms(0), resident(false), cost_bytes(0) {}

    CacheEntry(CacheKey key, CacheValue val, int64_t now_ms)
        : cache_key(std::move(key))
        , value(std::move(val))
        , freq(1)
        , last_access_ms(now_ms)
        , resident(true)
        , cost_bytes(0)
    {
        // Rough cost: key string + payload + fixed struct overhead
        cost_bytes = cache_key.ns.size() + cache_key.key.size()
                   + value.payload.size() + 128 /* struct overhead */;
    }

    void Touch(int64_t now_ms) {
        ++freq;
        last_access_ms = now_ms;
    }

    void IncrementFreq() { ++freq; }
};

} // namespace mercury
