#pragma once
// TinyLFU Admission Policy
// Reference: "TinyLFU: A Highly Efficient Cache Admission Policy"
//            Einziger & Friedman, ACM ToS 2017
//            https://dl.acm.org/doi/10.1145/3149371
//
// Architecture:
//   1. Doorkeeper (Bloom filter): cheap approximate "seen at least once" gate.
//   2. Count-Min Sketch: frequency estimator for items that passed the doorkeeper.
//   3. Admission decision: admit incoming item only if its estimated frequency
//      exceeds the victim's estimated frequency.
//   4. Periodic aging: halve all counters to shift focus toward recent accesses
//      (W-TinyLFU variant).

#include "../cache_core/CacheKey.h"
#include "../cache_core/CacheEntry.h"
#include <vector>
#include <array>
#include <cstdint>
#include <string>
#include <functional>

namespace mercury {

// ---------------------------------------------------------------------------
// Count-Min Sketch
// ---------------------------------------------------------------------------
class CountMinSketch {
public:
    explicit CountMinSketch(size_t width = 2048, size_t depth = 4)
        : width_(width), depth_(depth), table_(depth, std::vector<uint32_t>(width, 0))
    {}

    void Increment(const CacheKey& key) {
        for (size_t d = 0; d < depth_; ++d) {
            size_t col = HashAt(key, d) % width_;
            if (table_[d][col] < UINT32_MAX) ++table_[d][col];
        }
        ++total_additions_;
    }

    uint32_t Estimate(const CacheKey& key) const {
        uint32_t min_val = UINT32_MAX;
        for (size_t d = 0; d < depth_; ++d) {
            size_t col = HashAt(key, d) % width_;
            min_val = std::min(min_val, table_[d][col]);
        }
        return min_val;
    }

    // Halve all counters (aging / reset mechanism in W-TinyLFU)
    void Age() {
        for (auto& row : table_)
            for (auto& cell : row)
                cell >>= 1;
        total_additions_ /= 2;
    }

    uint64_t TotalAdditions() const { return total_additions_; }

private:
    size_t HashAt(const CacheKey& key, size_t depth_idx) const {
        size_t h = std::hash<std::string>{}(key.ToString());
        // Mix with depth index to get independent hash functions
        h ^= (h >> 17) * (depth_idx + 1) * 0xbf58476d1ce4e5b9ULL;
        h ^= (h >> 31);
        return h;
    }

    size_t width_, depth_;
    std::vector<std::vector<uint32_t>> table_;
    uint64_t total_additions_ = 0;
};

// ---------------------------------------------------------------------------
// Simple Bloom Filter (doorkeeper)
// ---------------------------------------------------------------------------
class BloomFilter {
public:
    explicit BloomFilter(size_t bits = 8192, size_t hashes = 3)
        : bits_(bits), hashes_(hashes), data_((bits + 63) / 64, 0)
    {}

    void Add(const CacheKey& key) {
        for (size_t i = 0; i < hashes_; ++i)
            data_[BitPos(key, i) / 64] |= (1ULL << (BitPos(key, i) % 64));
    }

    bool MightContain(const CacheKey& key) const {
        for (size_t i = 0; i < hashes_; ++i)
            if (!(data_[BitPos(key, i) / 64] & (1ULL << (BitPos(key, i) % 64))))
                return false;
        return true;
    }

    void Reset() { std::fill(data_.begin(), data_.end(), 0); }

private:
    size_t BitPos(const CacheKey& key, size_t idx) const {
        size_t h = std::hash<std::string>{}(key.ToString());
        h ^= (h >> 13) * (idx + 1) * 0x94d049bb133111ebULL;
        return h % bits_;
    }

    size_t bits_, hashes_;
    std::vector<uint64_t> data_;
};

// ---------------------------------------------------------------------------
// TinyLFU Admission Policy
// ---------------------------------------------------------------------------
class TinyLfuAdmissionPolicy {
public:
    // sample_size: number of accesses before triggering aging
    explicit TinyLfuAdmissionPolicy(size_t sample_size = 100000)
        : sample_size_(sample_size)
    {}

    // Called on every cache access (hit OR miss)
    void RecordAccess(const CacheKey& key) {
        if (!doorkeeper_.MightContain(key)) {
            doorkeeper_.Add(key);
        } else {
            sketch_.Increment(key);
        }
        ++access_count_;
        if (access_count_ >= sample_size_) {
            sketch_.Age();
            doorkeeper_.Reset();
            access_count_ = 0;
        }
    }

    void RecordMiss(const CacheKey& key) {
        // Treat miss as an access signal so misses can warm up the sketch
        RecordAccess(key);
    }

    // Returns true if the incoming entry should replace the victim.
    // Based on TinyLFU: admit if estimated freq(incoming) >= freq(victim).
    bool ShouldAdmit(const CacheEntry& incoming, const CacheEntry& victim) const {
        uint32_t inc_freq = sketch_.Estimate(incoming.cache_key);
        uint32_t vic_freq = sketch_.Estimate(victim.cache_key);
        return inc_freq >= vic_freq;
    }

    uint32_t EstimateFrequency(const CacheKey& key) const {
        return sketch_.Estimate(key);
    }

private:
    CountMinSketch sketch_;
    BloomFilter    doorkeeper_;
    size_t         sample_size_;
    uint64_t       access_count_ = 0;
};

} // namespace mercury
