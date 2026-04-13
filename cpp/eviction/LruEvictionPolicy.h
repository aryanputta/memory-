#pragma once
#include "IEvictionPolicy.h"
#include <list>
#include <unordered_map>

namespace mercury {

// Classic O(1) LRU implemented with a doubly-linked list + hash map.
// Most-recently-used = back of list; Least-recently-used = front.
class LruEvictionPolicy : public IEvictionPolicy {
public:
    LruEvictionPolicy() = default;

    void OnGet(CacheEntry& entry) override {
        auto it = positions_.find(entry.cache_key);
        if (it != positions_.end()) {
            // Move to back (MRU end)
            order_.erase(it->second);
            order_.push_back(entry.cache_key);
            it->second = std::prev(order_.end());
        }
    }

    void OnInsert(CacheEntry& entry) override {
        auto it = positions_.find(entry.cache_key);
        if (it != positions_.end()) {
            // Update existing position: move to back
            order_.erase(it->second);
        }
        order_.push_back(entry.cache_key);
        positions_[entry.cache_key] = std::prev(order_.end());
    }

    void OnDelete(const CacheEntry& entry) override {
        auto it = positions_.find(entry.cache_key);
        if (it != positions_.end()) {
            order_.erase(it->second);
            positions_.erase(it);
        }
    }

    std::optional<CacheKey> SelectVictim() override {
        if (order_.empty()) return std::nullopt;
        return order_.front(); // LRU end
    }

    std::string Name() const override { return "LRU"; }

private:
    std::list<CacheKey>                                            order_;
    std::unordered_map<CacheKey, std::list<CacheKey>::iterator>    positions_;
};

} // namespace mercury
