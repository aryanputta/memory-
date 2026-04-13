#pragma once
// CLOCK-Pro Eviction Policy
// ==========================
// Reference: "CLOCK-Pro: An Effective Improvement of the CLOCK Replacement"
//            Jiang, Chen & Zhang — USENIX ATC 2005
//            https://www.usenix.org/legacy/events/usenix05/tech/general/full_papers/jiang/jiang.pdf
//
// WHY THIS IS STRONGER THAN LRU for Amazon-style workloads:
//   - LRU is blind to access frequency; a one-time sequential scan evicts
//     hot resident pages (the "scan resistance" problem).
//   - CLOCK-Pro maintains three hands over a circular buffer of "hot",
//     "cold", and "non-resident" pages.
//   - Non-resident ghost entries record recently-evicted keys without
//     storing their values; if a ghost key is re-accessed, it is promoted
//     to hot — capturing recurrence not captured by pure LRU.
//   - This mirrors the "ghost buffer" concept in modern industry caches
//     (Caffeine for Java, S3-FIFO for Rust).
//
// Implementation summary:
//   - Cold pages: recently inserted, low frequency.
//   - Hot pages:  re-accessed while cold → promoted to hot ring.
//   - Ghost list: small set of recently evicted CacheKeys (no values stored).
//     Re-access of a ghost entry means the workload is recurrent; the key
//     gets admitted directly to hot on next insertion.

#include "IEvictionPolicy.h"
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <string>

namespace mercury {

enum class ClockProStatus { Hot, Cold, Ghost };

struct ClockProEntry {
    CacheKey         key;
    ClockProStatus   status      = ClockProStatus::Cold;
    bool             referenced  = false;  // R bit — set on access
};

class ClockProEvictionPolicy : public IEvictionPolicy {
public:
    // max_ghost: number of recently-evicted ghost entries to remember
    explicit ClockProEvictionPolicy(size_t max_ghost = 1024)
        : max_ghost_(max_ghost) {}

    void OnGet(CacheEntry& entry) override {
        auto it = pos_.find(entry.cache_key);
        if (it == pos_.end()) return;
        it->second->referenced = true;
        // If currently Cold → promote to Hot on next clock tick
    }

    void OnInsert(CacheEntry& entry) override {
        auto it = pos_.find(entry.cache_key);
        if (it != pos_.end()) {
            // Update: just set referenced bit
            it->second->referenced = true;
            return;
        }

        // Check if this key was recently evicted (ghost hit → admit as Hot)
        bool ghost_hit = ghost_set_.count(entry.cache_key) > 0;
        if (ghost_hit) ghost_set_.erase(entry.cache_key);

        ClockProEntry ce;
        ce.key        = entry.cache_key;
        ce.status     = ghost_hit ? ClockProStatus::Hot : ClockProStatus::Cold;
        ce.referenced = false;

        clock_.push_back(ce);
        pos_[entry.cache_key] = std::prev(clock_.end());

        if (ghost_hit) ++hot_count_;
        else           ++cold_count_;
    }

    void OnDelete(const CacheEntry& entry) override {
        RemoveFromClock(entry.cache_key);
    }

    // Returns the best eviction candidate (Cold, R=0)
    std::optional<CacheKey> SelectVictim() override {
        if (clock_.empty()) return std::nullopt;

        // Run the Cold hand: scan for a Cold entry with R=0
        size_t scanned = 0;
        size_t n       = clock_.size();
        while (scanned++ < 2 * n) {
            auto& ce = *cold_hand_;
            AdvanceColdHand();

            if (ce.status == ClockProStatus::Hot) {
                if (ce.referenced) {
                    ce.referenced = false; // give Hot a second chance
                }
                continue;
            }

            if (ce.status == ClockProStatus::Cold) {
                if (ce.referenced) {
                    // Promote Cold → Hot
                    ce.status     = ClockProStatus::Hot;
                    ce.referenced = false;
                    ++hot_count_;
                    --cold_count_;
                } else {
                    // Evict: move to ghost list
                    CacheKey victim = ce.key;
                    AddToGhost(victim);
                    return victim;
                }
            }
        }
        // Fallback: evict first entry
        return clock_.empty() ? std::nullopt
                               : std::optional<CacheKey>(clock_.front().key);
    }

    std::string Name() const override { return "CLOCK-Pro"; }

    size_t HotCount()  const { return hot_count_; }
    size_t ColdCount() const { return cold_count_; }
    size_t GhostCount() const { return ghost_set_.size(); }

private:
    using Ring     = std::list<ClockProEntry>;
    using RingIter = Ring::iterator;

    void AdvanceColdHand() {
        ++cold_hand_;
        if (cold_hand_ == clock_.end()) cold_hand_ = clock_.begin();
    }

    void RemoveFromClock(const CacheKey& key) {
        auto it = pos_.find(key);
        if (it == pos_.end()) return;
        auto& entry = *it->second;
        if (entry.status == ClockProStatus::Hot)  --hot_count_;
        if (entry.status == ClockProStatus::Cold) --cold_count_;
        if (cold_hand_ == it->second) AdvanceColdHand();
        clock_.erase(it->second);
        pos_.erase(it);
    }

    void AddToGhost(const CacheKey& key) {
        // Evict from clock first
        RemoveFromClock(key);
        ghost_set_.insert(key);

        // Trim ghost list if too large
        if (ghost_list_.size() >= max_ghost_) {
            ghost_set_.erase(ghost_list_.front());
            ghost_list_.pop_front();
        }
        ghost_list_.push_back(key);
    }

    Ring       clock_;
    RingIter   cold_hand_ = clock_.begin();
    std::unordered_map<CacheKey, RingIter> pos_;

    // Ghost state (no values — just keys)
    std::list<CacheKey>            ghost_list_;
    std::unordered_set<CacheKey>   ghost_set_;

    size_t max_ghost_ = 1024;
    size_t hot_count_ = 0;
    size_t cold_count_ = 0;
};

} // namespace mercury
