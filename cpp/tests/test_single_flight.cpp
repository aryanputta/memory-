#include <gtest/gtest.h>
#include "../concurrency/SingleFlightTable.h"
#include <thread>
#include <atomic>
#include <string>

using namespace mercury;

TEST(SingleFlight, OnlyOneLoaderExecuted) {
    SingleFlightTable<std::string> sf;
    std::atomic<int> loader_count{0};

    CacheKey key{"ns", "shared_key"};

    auto loader = [&](const CacheKey&) -> std::optional<std::string> {
        ++loader_count;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return std::string("loaded_value");
    };

    constexpr int kThreads = 10;
    std::vector<std::thread> threads;
    std::vector<std::optional<std::string>> results(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i]() {
            auto [val, was_leader] = sf.Do(key, loader);
            results[i] = val;
        });
    }

    for (auto& t : threads) t.join();

    // Loader should have been called exactly once
    EXPECT_EQ(loader_count.load(), 1);

    // All threads should have received the same value
    for (const auto& r : results) {
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(*r, "loaded_value");
    }
}

TEST(SingleFlight, DifferentKeysAllowConcurrentLoads) {
    SingleFlightTable<std::string> sf;
    std::atomic<int> loader_count{0};

    auto loader = [&](const CacheKey& k) -> std::optional<std::string> {
        ++loader_count;
        return k.key + "_value";
    };

    CacheKey k1{"ns", "key1"};
    CacheKey k2{"ns", "key2"};

    auto [v1, l1] = sf.Do(k1, loader);
    auto [v2, l2] = sf.Do(k2, loader);

    EXPECT_EQ(*v1, "key1_value");
    EXPECT_EQ(*v2, "key2_value");
    EXPECT_EQ(loader_count.load(), 2);
}

TEST(SingleFlight, SecondCallAfterCompletionTriggersNewLoad) {
    SingleFlightTable<std::string> sf;
    std::atomic<int> call_count{0};
    CacheKey key{"ns", "k"};

    auto loader = [&](const CacheKey&) -> std::optional<std::string> {
        ++call_count;
        return std::string("val");
    };

    sf.Do(key, loader);
    sf.Do(key, loader); // new flight after first completed

    EXPECT_EQ(call_count.load(), 2);
}
