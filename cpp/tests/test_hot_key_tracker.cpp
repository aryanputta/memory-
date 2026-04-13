#include <gtest/gtest.h>
#include "../hotkeys/HotKeyTracker.h"

using namespace mercury;

TEST(HotKeyTracker, IdentifiesHotKey) {
    // threshold = 10 QPS, window = 1000ms
    HotKeyTracker tracker(10, 1000, 5);

    CacheKey hot{"catalog", "product:1"};
    // Record enough accesses to exceed 10 * (1000/1000) = 10 accesses
    for (int i = 0; i < 100; ++i) tracker.RecordAccess(hot);

    EXPECT_TRUE(tracker.IsHot(hot));
}

TEST(HotKeyTracker, ColdKeyNotHot) {
    HotKeyTracker tracker(1000, 1000, 5);
    CacheKey cold{"catalog", "product:99"};
    tracker.RecordAccess(cold);
    EXPECT_FALSE(tracker.IsHot(cold));
}

TEST(HotKeyTracker, TopKReturnsCorrectOrder) {
    HotKeyTracker tracker(1, 1000, 3);

    CacheKey k1{"ns", "k1"};
    CacheKey k2{"ns", "k2"};
    CacheKey k3{"ns", "k3"};

    for (int i = 0; i < 30; ++i) tracker.RecordAccess(k1);
    for (int i = 0; i < 20; ++i) tracker.RecordAccess(k2);
    for (int i = 0; i < 10; ++i) tracker.RecordAccess(k3);

    auto top = tracker.TopK();
    ASSERT_FALSE(top.empty());
    // Top entry should be k1 (most accesses)
    EXPECT_EQ(top[0].key.key, "k1");
}
