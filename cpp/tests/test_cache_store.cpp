#include <gtest/gtest.h>
#include "../cache_core/CacheStore.h"
#include "../eviction/LruEvictionPolicy.h"
#include "../eviction/TinyLfuAdmissionPolicy.h"
#include <chrono>

using namespace mercury;

static int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

static CacheStore MakeStore(size_t max_bytes = 64 * 1024 /* 64 KB */) {
    auto lru = std::make_unique<LruEvictionPolicy>();
    auto tlfu = std::make_unique<TinyLfuAdmissionPolicy>();
    return CacheStore(max_bytes, std::move(lru), std::move(tlfu));
}

// ---------------------------------------------------------------------------
TEST(CacheStore, GetMissonEmpty) {
    auto store = MakeStore();
    auto result = store.Get({"catalog", "product:1"}, NowMs());
    EXPECT_FALSE(result.has_value());
}

TEST(CacheStore, PutAndGet) {
    auto store = MakeStore();
    int64_t now = NowMs();
    CacheValue val("hello", 1, now, 60000 /* 60s TTL */);
    RequestContext ctx;
    ctx.now_ms = now;

    auto res = store.Put({"catalog", "product:1"}, val, ctx);
    EXPECT_EQ(res.status, PutResultStatus::Admitted);

    auto got = store.Get({"catalog", "product:1"}, now);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->payload, "hello");
    EXPECT_EQ(got->version, 1);
}

TEST(CacheStore, UpdateExistingKey) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    store.Put({"ns", "k"}, CacheValue("v1", 1, now, 60000), ctx);
    auto res2 = store.Put({"ns", "k"}, CacheValue("v2", 2, now, 60000), ctx);
    EXPECT_EQ(res2.status, PutResultStatus::Updated);

    auto got = store.Get({"ns", "k"}, now);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->payload, "v2");
    EXPECT_EQ(got->version, 2);
}

TEST(CacheStore, DeleteKey) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    store.Put({"ns", "k"}, CacheValue("v", 1, now, 60000), ctx);
    EXPECT_TRUE(store.Delete({"ns", "k"}));
    EXPECT_FALSE(store.Get({"ns", "k"}, now).has_value());
    EXPECT_FALSE(store.Delete({"ns", "k"})); // already gone
}

TEST(CacheStore, TTLExpiry) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    // TTL of 1 ms — immediately expired
    store.Put({"ns", "k"}, CacheValue("v", 1, now, 1), ctx);
    // Access after expiry
    auto got = store.Get({"ns", "k"}, now + 100);
    EXPECT_FALSE(got.has_value());
}

TEST(CacheStore, NoTTLNeverExpires) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    store.Put({"ns", "k"}, CacheValue("v", 1, now, 0 /* no TTL */), ctx);
    // Far future: should still be present
    auto got = store.Get({"ns", "k"}, now + 1000000000LL);
    ASSERT_TRUE(got.has_value());
}

TEST(CacheStore, EvictionUnderMemoryPressure) {
    // Small store: only fits a few items
    auto store = MakeStore(512 /* 512 bytes */);
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    // Each entry ~128+ bytes overhead; insert many to trigger eviction
    for (int i = 0; i < 20; ++i) {
        std::string key = "k" + std::to_string(i);
        store.Put({"ns", key}, CacheValue("value_data_here", 1, now, 60000), ctx);
    }
    auto stats = store.SnapshotStats();
    // Memory should be within limit
    EXPECT_LE(stats.bytes_used, 512ULL);
    EXPECT_GT(stats.eviction_count, 0ULL);
}

TEST(CacheStore, CleanupExpired) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    store.Put({"ns", "expired1"}, CacheValue("v", 1, now, 1), ctx);
    store.Put({"ns", "expired2"}, CacheValue("v", 1, now, 1), ctx);
    store.Put({"ns", "alive"},    CacheValue("v", 1, now, 60000), ctx);

    size_t removed = store.CleanupExpired(now + 1000);
    EXPECT_EQ(removed, 2ULL);
    EXPECT_TRUE(store.Get({"ns", "alive"}, now).has_value());
}

TEST(CacheStore, BatchGet) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    store.Put({"catalog", "p:1"}, CacheValue("a", 1, now, 60000), ctx);
    store.Put({"catalog", "p:2"}, CacheValue("b", 2, now, 60000), ctx);

    std::vector<CacheKey> keys = {{"catalog", "p:1"},
                                  {"catalog", "p:2"},
                                  {"catalog", "p:3"}};
    auto results = store.BatchGet(keys, now);
    ASSERT_EQ(results.size(), 3ULL);
    EXPECT_TRUE(results[0].second.has_value());
    EXPECT_TRUE(results[1].second.has_value());
    EXPECT_FALSE(results[2].second.has_value());
}

TEST(CacheStore, HitRateStats) {
    auto store = MakeStore();
    int64_t now = NowMs();
    RequestContext ctx; ctx.now_ms = now;

    store.Put({"ns", "k"}, CacheValue("v", 1, now, 60000), ctx);
    store.Get({"ns", "k"}, now);  // hit
    store.Get({"ns", "x"}, now);  // miss

    auto stats = store.SnapshotStats();
    EXPECT_EQ(stats.hit_count,  1ULL);
    EXPECT_EQ(stats.miss_count, 1ULL);
    EXPECT_DOUBLE_EQ(stats.hit_rate(), 0.5);
}
