#pragma once
#include <string>
#include <functional>

namespace mercury {

struct CacheKey {
    std::string ns;   // namespace, e.g. "catalog"
    std::string key;  // key within namespace, e.g. "product:123"

    CacheKey() = default;
    CacheKey(std::string ns_, std::string key_)
        : ns(std::move(ns_)), key(std::move(key_)) {}

    // Combine namespace and key for storage/hashing
    std::string ToString() const { return ns + ":" + key; }

    bool operator==(const CacheKey& o) const {
        return ns == o.ns && key == o.key;
    }

    bool operator!=(const CacheKey& o) const { return !(*this == o); }
};

} // namespace mercury

// Custom hash so CacheKey can be used in unordered containers
namespace std {
template <>
struct hash<mercury::CacheKey> {
    size_t operator()(const mercury::CacheKey& ck) const noexcept {
        size_t h1 = std::hash<std::string>{}(ck.ns);
        size_t h2 = std::hash<std::string>{}(ck.key);
        // FNV-inspired combine
        return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};
} // namespace std
