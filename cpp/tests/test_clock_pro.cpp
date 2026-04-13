#include <gtest/gtest.h>
#include "../eviction/ClockProEvictionPolicy.h"

using namespace mercury;

static CacheEntry MakeCE(const std::string& k, int64_t now_ms = 0)
{
    CacheEntry e;
    e.cache_key     = {"ns", k};
    e.last_access_ms = now_ms;
    e.freq           = 1;
    e.resident       = true;
    e.cost_bytes     = 128;
    return e;
}

TEST(ClockPro, InsertAndSelectVictim) {
    ClockProEvictionPolicy cp;
    auto e1 = MakeCE("k1");
    auto e2 = MakeCE("k2");
    auto e3 = MakeCE("k3");

    cp.OnInsert(e1);
    cp.OnInsert(e2);
    cp.OnInsert(e3);

    auto victim = cp.SelectVictim();
    EXPECT_TRUE(victim.has_value());
    // All cold, R=0 — first cold entry should be chosen
}

TEST(ClockPro, ReferencedEntryGetsSecondChance) {
    ClockProEvictionPolicy cp;
    auto e1 = MakeCE("k1");
    auto e2 = MakeCE("k2");

    cp.OnInsert(e1);
    cp.OnInsert(e2);

    // Mark k1 as referenced
    cp.OnGet(e1);

    // k1 has R=1, so it should NOT be the first victim
    auto victim = cp.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->key, "k2"); // k2 is cold, R=0
}

TEST(ClockPro, GhostHitPromotesToHot) {
    ClockProEvictionPolicy cp(/*max_ghost=*/100);

    // Insert and evict k1 (becomes ghost)
    auto e1 = MakeCE("k1");
    cp.OnInsert(e1);
    auto v = cp.SelectVictim(); // evicts k1 → ghost
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v->key, "k1");

    // Re-insert k1 (ghost hit → should be admitted as Hot)
    auto e1_new = MakeCE("k1");
    cp.OnInsert(e1_new);

    EXPECT_GE(cp.HotCount(), 1u); // k1 promoted to hot
}

TEST(ClockPro, DeleteRemovesEntry) {
    ClockProEvictionPolicy cp;
    auto e1 = MakeCE("k1");
    auto e2 = MakeCE("k2");

    cp.OnInsert(e1);
    cp.OnInsert(e2);
    cp.OnDelete(e1);

    auto victim = cp.SelectVictim();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->key, "k2");
}

TEST(ClockPro, EmptyReturnsNullopt) {
    ClockProEvictionPolicy cp;
    EXPECT_FALSE(cp.SelectVictim().has_value());
}
