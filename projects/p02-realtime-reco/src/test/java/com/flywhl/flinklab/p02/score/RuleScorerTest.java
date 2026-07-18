package com.flywhl.flinklab.p02.score;

import com.flywhl.flinklab.p02.model.CatalogItem;
import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则加权 Top-K 打分契约（D-04 / RECO-02）。
 *
 * <p>权重锁定 RESEARCH Discretion：VIEW=1, CLICK=3, CART=5, BUY=10；类目匹配 ×1.5；Top-K=5。
 *
 * <p>Wave 0 RED：引用尚未交付的 {@link RuleScorer} / {@link FeatureSnapshot} / {@link CatalogItem}。
 */
class RuleScorerTest {

    @Test
    void ranksTopKByWeightedScoreDescending() {
        // 用户近期偏爱 electronics；BUY 权重应显著高于 VIEW
        FeatureSnapshot features = new FeatureSnapshot(
                "u-1",
                Map.of("electronics", 10.0, "books", 1.0),
                Map.of("i-buy", 10.0, "i-view", 1.0),
                1710000000000L);

        List<CatalogItem> catalog = List.of(
                new CatalogItem("i-buy", "electronics", "Phone", 1.0),
                new CatalogItem("i-view", "electronics", "Cable", 1.0),
                new CatalogItem("i-book", "books", "Novel", 1.0),
                new CatalogItem("i-e2", "electronics", "Case", 1.0),
                new CatalogItem("i-e3", "electronics", "Charger", 1.0),
                new CatalogItem("i-e4", "electronics", "Buds", 1.0),
                new CatalogItem("i-misc", "home", "Mug", 1.0));

        List<ScoredItem> top = RuleScorer.topK(features, catalog, 5);

        assertEquals(5, top.size(), "Top-K 默认 K=5");
        for (int i = 1; i < top.size(); i++) {
            assertTrue(
                    top.get(i - 1).score() >= top.get(i).score(),
                    "必须按加权分降序：" + top.get(i - 1) + " vs " + top.get(i));
        }
        assertEquals("i-buy", top.get(0).itemId(), "BUY 亲和 + 类目匹配应排第一");
    }

    @Test
    void buyWeightBeatsViewOnSameCategory() {
        FeatureSnapshot features = new FeatureSnapshot(
                "u-2",
                Map.of("electronics", 1.0),
                Map.of("i-a", 10.0, "i-b", 1.0),
                1710000000000L);

        List<CatalogItem> catalog = List.of(
                new CatalogItem("i-a", "electronics", "A", 1.0),
                new CatalogItem("i-b", "electronics", "B", 1.0));

        List<ScoredItem> top = RuleScorer.topK(features, catalog, 2);
        assertEquals(2, top.size());
        assertEquals("i-a", top.get(0).itemId(), "BUY=10 权重须高于 VIEW=1");
        assertTrue(top.get(0).score() > top.get(1).score());
    }

    @Test
    void categoryMatchBoostsScore() {
        FeatureSnapshot features = new FeatureSnapshot(
                "u-3",
                Map.of("electronics", 5.0),
                Map.of(),
                1710000000000L);

        List<CatalogItem> catalog = List.of(
                new CatalogItem("i-match", "electronics", "Match", 1.0),
                new CatalogItem("i-other", "books", "Other", 1.0));

        List<ScoredItem> top = RuleScorer.topK(features, catalog, 2);
        assertEquals("i-match", top.get(0).itemId(), "类目匹配 ×1.5 应抬升 electronics");
        assertTrue(top.get(0).score() > top.get(1).score());
    }
}
