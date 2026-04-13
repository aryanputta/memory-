#include "CacheStore.h"
#include <chrono>
#include <stdexcept>

namespace mercury {

CacheStore::CacheStore(size_t max_bytes,
                       std::unique_ptr<IEvictionPolicy> eviction,
                       std::unique_ptr<TinyLfuAdmissionPolicy> admission)
    : max_bytes_(max_bytes)
    , eviction_(std::move(eviction))
    , admission_(std::move(admission))
{}

// ---------------------------------------------------------------------------
// GET
// ---------------------------------------------------------------------------
std::optional<CacheValue> CacheStore::Get(const CacheKey& key, int64_t now_ms) {
    std::shared_lock<std::shared_mutex> rlock(mutex_);

    auto it = store_.find(key);
    if (it == store_.end()) {
        ++miss_count_;
        if (admission_) admission_->RecordMiss(key);
        return std::nullopt;
    }

    CacheEntry& entry = it->second;

    // Hard TTL check
    if (entry.value.IsExpired(now_ms)) {
        ++miss_count_;
        ++expired_count_;
        // Note: actual removal deferred to CleanupExpired() to avoid
        // lock upgrade from read -> write on hot path.
        return std::nullopt;
    }

    // Mark stale if soft-TTL has passed (stale-while-revalidate pattern)
    // Soft TTL = 80% of remaining hard TTL
    int64_t soft_window_ms = 0;
    if (entry.value.expires_at_ms > 0) {
        int64_t total_ttl = entry.value.expires_at_ms - entry.value.created_at_ms;
        soft_window_ms = total_ttl / 5; // last 20% of TTL is "soft expired"
    }
    entry.value.is_stale = entry.value.IsSoftExpired(now_ms, soft_window_ms);

    entry.Touch(now_ms);
    eviction_->OnGet(entry);
    if (admission_) admission_->RecordAccess(key);

    ++hit_count_;
    return entry.value;
}

// ---------------------------------------------------------------------------
// PUT
// ---------------------------------------------------------------------------
PutResult CacheStore::Put(const CacheKey& key, CacheValue value,
                          const RequestContext& ctx) {
    std::unique_lock<std::shared_mutex> wlock(mutex_);

    CacheEntry new_entry(key, std::move(value), ctx.now_ms);
    size_t needed = new_entry.cost_bytes;

    // TinyLFU admission gate when cache is full
    if (current_bytes_ + needed > max_bytes_) {
        if (admission_) {
            // Select the candidate victim according to eviction policy
            std::optional<CacheKey> victim_key = eviction_->SelectVictim();
            if (victim_key.has_value()) {
                auto vit = store_.find(*victim_key);
                bool should_admit = true;
                if (vit != store_.end()) {
                    should_admit = admission_->ShouldAdmit(new_entry, vit->second);
                }
                if (!should_admit) {
                    ++admission_rejections_;
                    return {PutResultStatus::Rejected, ""};
                }
            }
        }
        EvictUntilFits(needed, ctx);
    }

    // Check if we are updating an existing entry
    auto it = store_.find(key);
    if (it != store_.end()) {
        size_t old_cost = it->second.cost_bytes;
        it->second.value     = new_entry.value;
        it->second.cost_bytes = new_entry.cost_bytes;
        it->second.Touch(ctx.now_ms);
        eviction_->OnInsert(it->second);
        current_bytes_ = current_bytes_ - old_cost + new_entry.cost_bytes;
        return {PutResultStatus::Updated, ""};
    }

    // Fresh insert
    current_bytes_ += needed;
    auto& stored = store_[key];
    stored = std::move(new_entry);
    eviction_->OnInsert(stored);
    if (admission_) admission_->RecordAccess(key);

    return {PutResultStatus::Admitted, ""};
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------
bool CacheStore::Delete(const CacheKey& key) {
    std::unique_lock<std::shared_mutex> wlock(mutex_);

    auto it = store_.find(key);
    if (it == store_.end()) return false;

    current_bytes_ -= it->second.cost_bytes;
    eviction_->OnDelete(it->second);
    store_.erase(it);
    return true;
}

// ---------------------------------------------------------------------------
// BATCH GET
// ---------------------------------------------------------------------------
std::vector<std::pair<CacheKey, std::optional<CacheValue>>>
CacheStore::BatchGet(const std::vector<CacheKey>& keys, int64_t now_ms) {
    std::shared_lock<std::shared_mutex> rlock(mutex_);

    std::vector<std::pair<CacheKey, std::optional<CacheValue>>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        auto it = store_.find(key);
        if (it == store_.end() || it->second.value.IsExpired(now_ms)) {
            ++miss_count_;
            results.emplace_back(key, std::nullopt);
        } else {
            it->second.Touch(now_ms);
            eviction_->OnGet(it->second);
            ++hit_count_;
            results.emplace_back(key, it->second.value);
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// CLEANUP EXPIRED
// ---------------------------------------------------------------------------
size_t CacheStore::CleanupExpired(int64_t now_ms) {
    std::unique_lock<std::shared_mutex> wlock(mutex_);

    size_t removed = 0;
    for (auto it = store_.begin(); it != store_.end(); ) {
        if (it->second.value.IsExpired(now_ms)) {
            current_bytes_ -= it->second.cost_bytes;
            eviction_->OnDelete(it->second);
            it = store_.erase(it);
            ++removed;
            ++expired_count_;
        } else {
            ++it;
        }
    }
    return removed;
}

// ---------------------------------------------------------------------------
// EVICT UNTIL FITS (called under write lock)
// ---------------------------------------------------------------------------
void CacheStore::EvictUntilFits(size_t needed_bytes, const RequestContext& /*ctx*/) {
    while (current_bytes_ + needed_bytes > max_bytes_) {
        std::optional<CacheKey> victim = eviction_->SelectVictim();
        if (!victim.has_value()) break; // nothing left to evict

        auto it = store_.find(*victim);
        if (it == store_.end()) break;

        current_bytes_ -= it->second.cost_bytes;
        eviction_->OnDelete(it->second);
        store_.erase(it);
        ++eviction_count_;
    }
}

// ---------------------------------------------------------------------------
// STATS
// ---------------------------------------------------------------------------
StoreStats CacheStore::SnapshotStats() const {
    std::shared_lock<std::shared_mutex> rlock(mutex_);
    StoreStats s{};
    s.item_count          = store_.size();
    s.bytes_used          = current_bytes_.load();
    s.bytes_limit         = max_bytes_;
    s.hit_count           = hit_count_.load();
    s.miss_count          = miss_count_.load();
    s.eviction_count      = eviction_count_.load();
    s.expired_count       = expired_count_.load();
    s.admission_rejections = admission_rejections_.load();
    return s;
}

bool CacheStore::Contains(const CacheKey& key) const {
    std::shared_lock<std::shared_mutex> rlock(mutex_);
    return store_.count(key) > 0;
}

} // namespace mercury
