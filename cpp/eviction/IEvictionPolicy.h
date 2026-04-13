#pragma once
#include "../cache_core/CacheEntry.h"
#include <optional>
#include <string>

namespace mercury {

// Interface every eviction policy must implement.
// All methods are called under the CacheStore's write lock, so they do NOT
// need to be individually thread-safe.
class IEvictionPolicy {
public:
    virtual ~IEvictionPolicy() = default;

    // Called after a successful cache hit
    virtual void OnGet(CacheEntry& entry) = 0;

    // Called when an entry is inserted or updated
    virtual void OnInsert(CacheEntry& entry) = 0;

    // Called when an entry is removed (eviction, explicit delete, TTL expiry)
    virtual void OnDelete(const CacheEntry& entry) = 0;

    // Return the key of the best eviction candidate, or nullopt if empty
    virtual std::optional<CacheKey> SelectVictim() = 0;

    virtual std::string Name() const = 0;
};

} // namespace mercury
