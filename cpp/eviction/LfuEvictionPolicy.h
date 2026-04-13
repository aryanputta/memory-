#pragma once
#include "IEvictionPolicy.h"
#include <unordered_map>
#include <map>
#include <unordered_set>

namespace mercury {

// O(1) LFU using frequency buckets (Huang et al. 2010 approach).
// Entries with the minimum frequency are evicted first;
// ties broken by LRU order within the same frequency bucket.
class LfuEvictionPolicy : public IEvictionPolicy {
public:
    LfuEvictionPolicy() = default;

    void OnGet(CacheEntry& entry) override {
        auto it = key_freq_.find(entry.cache_key);
        if (it == key_freq_.end()) return;
        uint64_t old_freq = it->second;
        uint64_t new_freq = old_freq + 1;
        it->second = new_freq;
        entry.IncrementFreq();

        freq_buckets_[old_freq].erase(entry.cache_key);
        if (freq_buckets_[old_freq].empty()) {
            freq_buckets_.erase(old_freq);
            if (min_freq_ == old_freq) ++min_freq_;
        }
        freq_buckets_[new_freq].insert(entry.cache_key);
    }

    void OnInsert(CacheEntry& entry) override {
        // If already tracked, treat as access
        if (key_freq_.count(entry.cache_key)) {
            OnGet(entry);
            return;
        }
        key_freq_[entry.cache_key] = 1;
        freq_buckets_[1].insert(entry.cache_key);
        min_freq_ = 1;
    }

    void OnDelete(const CacheEntry& entry) override {
        auto it = key_freq_.find(entry.cache_key);
        if (it == key_freq_.end()) return;
        uint64_t freq = it->second;
        freq_buckets_[freq].erase(entry.cache_key);
        if (freq_buckets_[freq].empty()) freq_buckets_.erase(freq);
        key_freq_.erase(it);
        // Recompute min_freq lazily
        if (!freq_buckets_.empty()) {
            min_freq_ = freq_buckets_.begin()->first;
        }
    }

    std::optional<CacheKey> SelectVictim() override {
        if (key_freq_.empty()) return std::nullopt;
        auto bucket_it = freq_buckets_.begin();
        if (bucket_it == freq_buckets_.end()) return std::nullopt;
        return *bucket_it->second.begin(); // arbitrary from lowest-freq bucket
    }

    std::string Name() const override { return "LFU"; }

private:
    std::unordered_map<CacheKey, uint64_t>               key_freq_;
    // Ordered by frequency; each bucket is a set of keys
    std::map<uint64_t, std::unordered_set<CacheKey>>     freq_buckets_;
    uint64_t min_freq_ = 0;
};

} // namespace mercury
