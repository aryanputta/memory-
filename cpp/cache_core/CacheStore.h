#pragma once
#include "CacheKey.h"
#include "CacheValue.h"
#include "CacheEntry.h"
#include "RequestContext.h"
#include "../eviction/IEvictionPolicy.h"
#include "../eviction/TinyLfuAdmissionPolicy.h"
#include <unordered_map>
#include <optional>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <atomic>
#include <cstdint>

namespace mercury {

enum class PutResultStatus { Admitted, Rejected, Updated };

struct PutResult {
    PutResultStatus status;
    std::string     evicted_key; // populated if an eviction occurred
};

struct StoreStats {
    uint64_t item_count;
    uint64_t bytes_used;
    uint64_t bytes_limit;
    uint64_t hit_count;
    uint64_t miss_count;
    uint64_t eviction_count;
    uint64_t expired_count;
    uint64_t admission_rejections;
    double   hit_rate() const {
        uint64_t total = hit_count + miss_count;
        return total > 0 ? static_cast<double>(hit_count) / total : 0.0;
    }
};

class CacheStore {
public:
    explicit CacheStore(size_t max_bytes,
                        std::unique_ptr<IEvictionPolicy> eviction,
                        std::unique_ptr<TinyLfuAdmissionPolicy> admission = nullptr);

    // Core operations -------------------------------------------------------
    std::optional<CacheValue> Get(const CacheKey& key, int64_t now_ms);

    PutResult Put(const CacheKey& key, CacheValue value, const RequestContext& ctx);

    bool Delete(const CacheKey& key);

    // Batch get (does not acquire per-key lock; uses a single read lock)
    std::vector<std::pair<CacheKey, std::optional<CacheValue>>>
    BatchGet(const std::vector<CacheKey>& keys, int64_t now_ms);

    // Maintenance -----------------------------------------------------------
    // Remove entries whose hard TTL has elapsed. Returns evicted count.
    size_t CleanupExpired(int64_t now_ms);

    StoreStats SnapshotStats() const;

    // Expose for hot-key tracker
    bool Contains(const CacheKey& key) const;

private:
    void EvictUntilFits(size_t needed_bytes, const RequestContext& ctx);

    std::unordered_map<CacheKey, CacheEntry> store_;
    std::unique_ptr<IEvictionPolicy>         eviction_;
    std::unique_ptr<TinyLfuAdmissionPolicy>  admission_;

    size_t max_bytes_;
    std::atomic<size_t> current_bytes_{0};

    // Counters
    std::atomic<uint64_t> hit_count_{0};
    std::atomic<uint64_t> miss_count_{0};
    std::atomic<uint64_t> eviction_count_{0};
    std::atomic<uint64_t> expired_count_{0};
    std::atomic<uint64_t> admission_rejections_{0};

    mutable std::shared_mutex mutex_;
};

} // namespace mercury
