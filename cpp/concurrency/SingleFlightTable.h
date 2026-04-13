#pragma once
// Single-flight (request coalescing) table.
// When multiple threads concurrently miss on the same key, only ONE thread
// fetches from the backing store; the others wait and receive the same result.
// This directly prevents the "thundering herd" problem described in the
// Amazon Builders' Library: https://aws.amazon.com/builders-library/caching-challenges-and-strategies/

#include "../cache_core/CacheKey.h"
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <condition_variable>
#include <mutex>
#include <functional>
#include <optional>
#include <string>

namespace mercury {

template <typename Value>
struct InFlight {
    bool                    done  = false;
    std::optional<Value>    result;
    std::string             error;
    std::mutex              mu;
    std::condition_variable cv;
};

template <typename Value>
class SingleFlightTable {
public:
    using Loader = std::function<std::optional<Value>(const CacheKey&)>;

    // Execute loader exactly once per key regardless of how many concurrent
    // calls arrive with the same key.
    // Returns {value, was_leader}:
    //   was_leader = true  → this goroutine executed the loader
    //   was_leader = false → this goroutine waited and received a shared result
    std::pair<std::optional<Value>, bool>
    Do(const CacheKey& key, Loader loader) {
        std::shared_ptr<InFlight<Value>> flight;
        bool is_leader = false;

        {
            std::unique_lock<std::mutex> tbl_lock(table_mu_);
            auto it = in_flight_.find(key);
            if (it != in_flight_.end()) {
                flight = it->second;
                is_leader = false;
            } else {
                flight = std::make_shared<InFlight<Value>>();
                in_flight_[key] = flight;
                is_leader = true;
            }
        }

        if (is_leader) {
            // Execute the loader outside the table lock
            std::optional<Value> result;
            try {
                result = loader(key);
            } catch (const std::exception& e) {
                std::unique_lock<std::mutex> lk(flight->mu);
                flight->error = e.what();
                flight->done  = true;
                flight->cv.notify_all();

                // Remove from table
                std::unique_lock<std::mutex> tbl_lock(table_mu_);
                in_flight_.erase(key);
                throw;
            }

            {
                std::unique_lock<std::mutex> lk(flight->mu);
                flight->result = result;
                flight->done   = true;
                flight->cv.notify_all();
            }

            // Remove from table so the next miss creates a fresh flight
            {
                std::unique_lock<std::mutex> tbl_lock(table_mu_);
                in_flight_.erase(key);
            }
            return {result, true};
        } else {
            // Wait for the leader to finish
            std::unique_lock<std::mutex> lk(flight->mu);
            flight->cv.wait(lk, [&]{ return flight->done; });
            if (!flight->error.empty()) {
                throw std::runtime_error(flight->error);
            }
            return {flight->result, false};
        }
    }

    size_t InFlightCount() const {
        std::unique_lock<std::mutex> tbl_lock(table_mu_);
        return in_flight_.size();
    }

private:
    mutable std::mutex table_mu_;
    std::unordered_map<CacheKey, std::shared_ptr<InFlight<Value>>> in_flight_;
};

} // namespace mercury
