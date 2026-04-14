#pragma once
// Lightweight in-process metrics registry.
// Exposes counters and histograms used by the gRPC server for Prometheus scraping.

#include <unordered_map>
#include <string>
#include <atomic>
#include <shared_mutex>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cmath>
#include <memory>
#include <sstream>

namespace mercury {

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------
class Counter {
public:
    void Inc(uint64_t delta = 1) { value_.fetch_add(delta, std::memory_order_relaxed); }
    uint64_t Value() const { return value_.load(std::memory_order_relaxed); }
private:
    std::atomic<uint64_t> value_{0};
};

// ---------------------------------------------------------------------------
// Histogram (powers-of-2 bucketing, suitable for latency in microseconds)
// ---------------------------------------------------------------------------
class Histogram {
public:
    explicit Histogram(size_t num_buckets = 20)
        : num_buckets_(num_buckets)
    {
        // std::atomic is not CopyConstructible, so the fill constructor
        // std::vector(n, val) cannot be used.  Resize then zero each slot.
        buckets_.resize(num_buckets);
        for (size_t i = 0; i < num_buckets; ++i)
            buckets_[i].store(0, std::memory_order_relaxed);
    }

    void Record(double value_us) {
        uint64_t rounded = static_cast<uint64_t>(std::max(0.0, value_us));
        size_t idx = 0;
        while (idx + 1 < num_buckets_ && rounded >= (1ULL << idx)) ++idx;
        buckets_[idx].fetch_add(1, std::memory_order_relaxed);
        // Running sum for mean
        sum_.fetch_add(static_cast<uint64_t>(value_us * 1000),
                       std::memory_order_relaxed); // stored in nano-fractions
        ++count_;
    }

    // Approximate percentile from bucket histogram
    double Percentile(double p) const {
        uint64_t total = count_.load();
        if (total == 0) return 0.0;
        uint64_t target = static_cast<uint64_t>(p * total);
        uint64_t cumul = 0;
        for (size_t i = 0; i < num_buckets_; ++i) {
            cumul += buckets_[i].load(std::memory_order_relaxed);
            if (cumul >= target) return static_cast<double>(1ULL << i);
        }
        return static_cast<double>(1ULL << (num_buckets_ - 1));
    }

    double Mean() const {
        uint64_t n = count_.load();
        return n > 0 ? static_cast<double>(sum_.load()) / n / 1000.0 : 0.0;
    }

    uint64_t Count() const { return count_.load(); }

private:
    std::vector<std::atomic<uint64_t>> buckets_;
    std::atomic<uint64_t> sum_{0};
    std::atomic<uint64_t> count_{0};
    size_t num_buckets_;
};

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------
class MetricsRegistry {
public:
    static MetricsRegistry& Global() {
        static MetricsRegistry instance;
        return instance;
    }

    Counter& GetCounter(const std::string& name) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        return *counters_.emplace(name, std::make_unique<Counter>())
                         .first->second;
    }

    Histogram& GetHistogram(const std::string& name) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        return *histograms_.emplace(name, std::make_unique<Histogram>())
                           .first->second;
    }

    // Emit Prometheus text format
    std::string PrometheusExport() const {
        std::shared_lock<std::shared_mutex> lk(mu_);
        std::ostringstream oss;
        for (const auto& [name, ctr] : counters_) {
            oss << "# HELP mercury_" << name << " counter\n";
            oss << "# TYPE mercury_" << name << " counter\n";
            oss << "mercury_" << name << " " << ctr->Value() << "\n";
        }
        for (const auto& [name, hist] : histograms_) {
            oss << "# HELP mercury_" << name << "_us latency histogram\n";
            oss << "# TYPE mercury_" << name << "_us summary\n";
            oss << "mercury_" << name << "_us{quantile=\"0.50\"} "
                << hist->Percentile(0.50) << "\n";
            oss << "mercury_" << name << "_us{quantile=\"0.95\"} "
                << hist->Percentile(0.95) << "\n";
            oss << "mercury_" << name << "_us{quantile=\"0.99\"} "
                << hist->Percentile(0.99) << "\n";
            oss << "mercury_" << name << "_us_count " << hist->Count() << "\n";
        }
        return oss.str();
    }

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<Counter>>   counters_;
    std::unordered_map<std::string, std::unique_ptr<Histogram>>  histograms_;
};

} // namespace mercury
