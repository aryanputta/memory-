#include <gtest/gtest.h>
#include "../eviction/LruEvictionPolicy.h"
#include "../eviction/LfuEvictionPolicy.h"
#include "../eviction/TinyLfuAdmissionPolicy.h"

using namespace mercury;

// ---------------------------------------------------------------------------
// Helper: construct a minimal CacheEntry
// ---------------------------------------------------------------------------
static CacheEntry MakeEntry(const std::string& ns, const std::string& key,
                             int64_t now_ms = 0, uint64_t freq = 1)
{
    CacheEntry e;
    e.cache_key     = {ns, key};
    e.last_access_ms = now_ms;
    e.freq           = freq;
    e.resident       = true;
    e.cost_bytes     = 128;
    return e;
}

// ===========================================================================
// LRU Tests
// ===========================================================================
TEST(LruEviction, SelectsLeastRecentlyUsed) {
    LruEvictionPolicy lru;
    auto e1 = MakeEntry("ns", "k1");
    auto e2 = MakeEntry("ns", "k2");
    auto e3 = MakeEntry("ns", "k3");
    lru.OnInsert(e1);
    lru.OnInsert(e2);
    lru.OnInsert(e3);
    // k1 is the LRU (inserted first, not accessed since)
    auto victim = lru.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->key, "k1");
}

TEST(LruEviction, AccessMovesToMru) {
    LruEvictionPolicy lru;
    auto e1 = MakeEntry("ns", "k1");
    auto e2 = MakeEntry("ns", "k2");
    lru.OnInsert(e1);
    lru.OnInsert(e2);
    // Access k1 — it should no longer be the LRU victim
    lru.OnGet(e1);
    auto victim = lru.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->key, "k2");
}

TEST(LruEviction, DeleteRemovesFromLru) {
    LruEvictionPolicy lru;
    auto e1 = MakeEntry("ns", "k1");
    auto e2 = MakeEntry("ns", "k2");
    lru.OnInsert(e1);
    lru.OnInsert(e2);
    lru.OnDelete(e1);
    auto victim = lru.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->key, "k2");
}

TEST(LruEviction, EmptyReturnsNullopt) {
    LruEvictionPolicy lru;
    EXPECT_FALSE(lru.SelectVictim().has_value());
}

// ===========================================================================
// LFU Tests
// ===========================================================================
TEST(LfuEviction, SelectsLeastFrequentlyUsed) {
    LfuEvictionPolicy lfu;
    auto e1 = MakeEntry("ns", "k1"); // freq=1
    auto e2 = MakeEntry("ns", "k2"); // freq=1 → will be bumped to 3
    auto e3 = MakeEntry("ns", "k3"); // freq=1

    lfu.OnInsert(e1);
    lfu.OnInsert(e2);
    lfu.OnInsert(e3);
    // Access k2 twice more
    lfu.OnGet(e2);
    lfu.OnGet(e2);

    // k1 and k3 have freq=1; k2 has freq=3
    // victim should be k1 or k3 (both freq=1)
    auto victim = lfu.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_NE(victim->key, "k2");
}

TEST(LfuEviction, DeleteUpdatesMinFreq) {
    LfuEvictionPolicy lfu;
    auto e1 = MakeEntry("ns", "k1");
    auto e2 = MakeEntry("ns", "k2");
    lfu.OnInsert(e1);
    lfu.OnInsert(e2);
    lfu.OnDelete(e1);
    auto victim = lfu.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->key, "k2");
}

// ===========================================================================
// TinyLFU Tests
// ===========================================================================
TEST(TinyLfu, AdmitsFrequentIncoming) {
    TinyLfuAdmissionPolicy tlfu;
    CacheKey hot{"ns", "popular"};
    CacheKey cold{"ns", "rare"};

    // Record many accesses for "popular"
    for (int i = 0; i < 100; ++i) tlfu.RecordAccess(hot);
    // "rare" accessed only once
    tlfu.RecordAccess(cold);

    auto incoming = MakeEntry("ns", "popular");
    auto victim   = MakeEntry("ns", "rare");

    // popular's frequency >> rare's → should admit
    EXPECT_TRUE(tlfu.ShouldAdmit(incoming, victim));
}

TEST(TinyLfu, RejectsInfrequentIncoming) {
    TinyLfuAdmissionPolicy tlfu;
    CacheKey hot{"ns", "hot_victim"};
    CacheKey cold{"ns", "new_item"};

    // Victim is very hot
    for (int i = 0; i < 200; ++i) tlfu.RecordAccess(hot);
    // Incoming is cold
    tlfu.RecordAccess(cold);

    auto incoming = MakeEntry("ns", "new_item");
    auto victim   = MakeEntry("ns", "hot_victim");

    EXPECT_FALSE(tlfu.ShouldAdmit(incoming, victim));
}

TEST(TinyLfu, EstimateFrequency) {
    TinyLfuAdmissionPolicy tlfu;
    CacheKey key{"ns", "k"};
    for (int i = 0; i < 50; ++i) tlfu.RecordAccess(key);
    EXPECT_GT(tlfu.EstimateFrequency(key), 0u);
}
