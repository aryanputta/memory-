#pragma once
// Hot Key Tracker
// Uses a sliding-window Count-Min Sketch + heavy-hitter heap to identify
// "hot" keys in real time. Hot keys get extra replicas in the C# layer
// to distribute read load, preventing noisy-neighbor problems (per AWS DAX
// cluster configuration guidance).

#include "../cache_core/CacheKey.h"
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdint>
#include <functional>

namespace mercury {

struct KeyStats {
    CacheKey key;
    uint64_t estimated_qps = 0;
    bool     is_hot        = false;
};

class HotKeyTracker {
public:
    // hot_threshold_qps: keys exceeding this rate per second are "hot"
    // window_ms: sliding window size in milliseconds
    explicit HotKeyTracker(uint64_t hot_threshold_qps = 1000,
                           uint64_t window_ms          = 60000,
                           size_t   top_k              = 20)
        : hot_threshold_qps_(hot_threshold_qps)
        , window_ms_(window_ms)
        , top_k_(top_k)
        , sketch_width_(4096)
        , sketch_depth_(4)
        , sketch_(sketch_depth_, std::vector<uint32_t>(sketch_width_, 0))
    {}

    void RecordAccess(const CacheKey& key) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        IncrementSketch(key);
        ++access_count_;
        uint64_t freq = EstimateSketch(key);
        freq_map_[key] = freq;
        MaybeAge();
    }

    bool IsHot(const CacheKey& key) const {
        std::shared_lock<std::shared_mutex> lk(mu_);
        auto it = freq_map_.find(key);
        if (it == freq_map_.end()) return false;
        // rough QPS = estimated accesses / window in seconds
        double window_secs = window_ms_ / 1000.0;
        uint64_t qps = static_cast<uint64_t>(it->second / window_secs);
        return qps >= hot_threshold_qps_;
    }

    // Returns top-K hot keys by estimated frequency
    std::vector<KeyStats> TopK() const {
        std::shared_lock<std::shared_mutex> lk(mu_);
        std::vector<std::pair<uint64_t, CacheKey>> ranked;
        ranked.reserve(freq_map_.size());
        for (const auto& [k, f] : freq_map_) ranked.emplace_back(f, k);
        std::partial_sort(ranked.begin(),
                          ranked.begin() + std::min(top_k_, ranked.size()),
                          ranked.end(),
                          [](const auto& a, const auto& b){ return a.first > b.first; });

        std::vector<KeyStats> result;
        double window_secs = window_ms_ / 1000.0;
        for (size_t i = 0; i < std::min(top_k_, ranked.size()); ++i) {
            KeyStats s;
            s.key           = ranked[i].second;
            s.estimated_qps = static_cast<uint64_t>(ranked[i].first / window_secs);
            s.is_hot        = s.estimated_qps >= hot_threshold_qps_;
            result.push_back(s);
        }
        return result;
    }

private:
    void IncrementSketch(const CacheKey& key) {
        for (size_t d = 0; d < sketch_depth_; ++d) {
            size_t col = HashAt(key, d) % sketch_width_;
            if (sketch_[d][col] < UINT32_MAX) ++sketch_[d][col];
        }
    }

    uint32_t EstimateSketch(const CacheKey& key) const {
        uint32_t min_val = UINT32_MAX;
        for (size_t d = 0; d < sketch_depth_; ++d) {
            size_t col = HashAt(key, d) % sketch_width_;
            min_val = std::min(min_val, sketch_[d][col]);
        }
        return min_val;
    }

    size_t HashAt(const CacheKey& key, size_t depth_idx) const {
        size_t h = std::hash<std::string>{}(key.ToString());
        h ^= (h >> 17) * (depth_idx + 1) * 0xbf58476d1ce4e5b9ULL;
        h ^= (h >> 31);
        return h;
    }

    // Halve sketch counters periodically to simulate sliding window
    void MaybeAge() {
        if (access_count_ >= 2 * sketch_width_) {
            for (auto& row : sketch_)
                for (auto& cell : row) cell >>= 1;
            for (auto& [k, f] : freq_map_) f >>= 1;
            access_count_ = 0;
        }
    }

    uint64_t hot_threshold_qps_;
    uint64_t window_ms_;
    size_t   top_k_;

    size_t sketch_width_, sketch_depth_;
    std::vector<std::vector<uint32_t>> sketch_;
    std::unordered_map<CacheKey, uint64_t> freq_map_;
    uint64_t access_count_ = 0;

    mutable std::shared_mutex mu_;
};

} // namespace mercury
