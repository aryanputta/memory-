#pragma once
// Striped locking: partition the key space across N independent mutexes.
// Reduces contention vs. a single global lock while still being simpler
// than fully lock-free data structures.

#include <mutex>
#include <shared_mutex>
#include <vector>
#include <functional>
#include "../cache_core/CacheKey.h"

namespace mercury {

template <typename Mutex = std::shared_mutex>
class StripedLock {
public:
    explicit StripedLock(size_t stripes = 64) : locks_(stripes) {}

    // Returns the index of the stripe for the given key
    size_t StripeOf(const CacheKey& key) const {
        size_t h = std::hash<CacheKey>{}(key);
        return h % locks_.size();
    }

    Mutex& LockFor(const CacheKey& key) {
        return locks_[StripeOf(key)];
    }

    const Mutex& LockFor(const CacheKey& key) const {
        return locks_[StripeOf(key)];
    }

    size_t NumStripes() const { return locks_.size(); }

private:
    // vector of mutexes — non-copyable, non-movable; wrap in unique_ptr
    std::vector<Mutex> locks_;
};

} // namespace mercury
