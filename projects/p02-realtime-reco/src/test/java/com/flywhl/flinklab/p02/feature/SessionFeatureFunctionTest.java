package com.flywhl.flinklab.p02.feature;

import com.flywhl.flinklab.p02.model.BehaviorEvent;
import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SessionFeature 纯逻辑契约（D-01）：包内辅助累积，避免强依赖 MiniCluster。
 */
class SessionFeatureFunctionTest {

    @Test
    void clickIncrementsCountAndItemAffinity() {
        BehaviorEvent click = new BehaviorEvent("u-1", "i-001", "CLICK", 1_710_000_000_000L);
        FeatureSnapshot snap = SessionFeatureFunction.applyEvent(null, click, "electronics");

        assertEquals("u-1", snap.userId());
        assertEquals(1L, snap.clickCount());
        assertEquals(3.0, snap.itemAffinity().get("i-001"), 1e-9);
        assertEquals(3.0, snap.categoryAffinity().get("electronics"), 1e-9);
        assertEquals(1_710_000_000_000L, snap.lastEventTs());
    }

    @Test
    void buyWeightBeatsViewOnSameItem() {
        FeatureSnapshot afterView = SessionFeatureFunction.applyEvent(
                null,
                new BehaviorEvent("u-2", "i-a", "VIEW", 100L),
                "electronics");
        FeatureSnapshot afterBuy = SessionFeatureFunction.applyEvent(
                afterView,
                new BehaviorEvent("u-2", "i-b", "BUY", 200L),
                "electronics");

        assertEquals(1.0, afterBuy.itemAffinity().get("i-a"), 1e-9);
        assertEquals(10.0, afterBuy.itemAffinity().get("i-b"), 1e-9);
        assertEquals(11.0, afterBuy.categoryAffinity().get("electronics"), 1e-9);
        assertEquals(200L, afterBuy.lastEventTs());
        assertEquals(0L, afterBuy.clickCount(), "VIEW/BUY 不计入 clickCount");
    }

    @Test
    void categoryAffinityAccumulatesAcrossEvents() {
        FeatureSnapshot s1 = SessionFeatureFunction.applyEvent(
                null,
                new BehaviorEvent("u-3", "i-1", "CART", 10L),
                "books");
        FeatureSnapshot s2 = SessionFeatureFunction.applyEvent(
                s1,
                new BehaviorEvent("u-3", "i-2", "CART", 20L),
                "books");

        assertEquals(10.0, s2.categoryAffinity().get("books"), 1e-9);
        assertTrue(s2.itemAffinity().containsKey("i-1"));
        assertTrue(s2.itemAffinity().containsKey("i-2"));
    }
}
