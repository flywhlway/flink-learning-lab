package com.flywhl.flinklab.p02.score;

import com.flywhl.flinklab.p02.model.CatalogItem;
import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import com.flywhl.flinklab.p02.model.RecoResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 可解释规则加权 Top-K 纯函数（D-04）。
 *
 * <p>权重 VIEW=1 CLICK=3 CART=5 BUY=10（已落入特征亲和）；类目匹配 ×1.5；
 * 近因半衰期 30min（相对 {@code nowMs}，缺省用 lastEventTs 使因子=1）。
 * 禁止 Milvus / LLM / 外部模型。
 */
public final class RuleScorer {

    /** 类目匹配加成。 */
    public static final double CATEGORY_BOOST = 1.5;

    /** 近因半衰期（毫秒）= 30 分钟。 */
    public static final long RECENCY_HALF_LIFE_MS = 30L * 60L * 1000L;

    private RuleScorer() {
    }

    /**
     * 对 catalog 打分并取 Top-K（降序）。
     */
    public static List<ScoredItem> topK(FeatureSnapshot features, List<CatalogItem> catalog, int k) {
        return topK(features, catalog, k, features == null ? 0L : features.lastEventTs());
    }

    public static List<ScoredItem> topK(
            FeatureSnapshot features, List<CatalogItem> catalog, int k, long nowMs) {
        if (features == null || catalog == null || catalog.isEmpty() || k <= 0) {
            return List.of();
        }
        List<ScoredItem> scored = new ArrayList<>(catalog.size());
        for (CatalogItem item : catalog) {
            if (item == null || item.itemId() == null) {
                continue;
            }
            scored.add(scoreOne(features, item, nowMs));
        }
        scored.sort(Comparator.comparingDouble(ScoredItem::score).reversed()
                .thenComparing(ScoredItem::itemId));
        if (scored.size() <= k) {
            return List.copyOf(scored);
        }
        return List.copyOf(scored.subList(0, k));
    }

    /**
     * 产出作业侧 {@link RecoResult} 列表。
     */
    public static List<RecoResult> scoreToResults(
            FeatureSnapshot features,
            List<CatalogItem> catalog,
            int k,
            String featureSource) {
        List<ScoredItem> top = topK(features, catalog, k);
        List<RecoResult> out = new ArrayList<>(top.size());
        String src = featureSource == null ? "STATE_ONLY" : featureSource;
        String snap = features == null ? "{}" : features.compactSummary();
        long ts = features == null ? System.currentTimeMillis() : features.lastEventTs();
        for (ScoredItem s : top) {
            out.add(new RecoResult(
                    features.userId(),
                    s.itemId(),
                    s.score(),
                    ts,
                    s.reason(),
                    src,
                    snap));
        }
        return out;
    }

    static ScoredItem scoreOne(FeatureSnapshot features, CatalogItem item, long nowMs) {
        Map<String, Double> itemAff = features.itemAffinity();
        Map<String, Double> catAff = features.categoryAffinity();
        double itemScore = itemAff == null ? 0.0 : itemAff.getOrDefault(item.itemId(), 0.0);
        double catScore = 0.0;
        if (catAff != null && item.category() != null) {
            catScore = catAff.getOrDefault(item.category(), 0.0);
        }
        boolean catMatch = catScore > 0.0;
        double catMul = catMatch ? CATEGORY_BOOST : 1.0;
        double recency = recencyFactor(features.lastEventTs(), nowMs);
        double base = item.baseWeight() <= 0 ? 1.0 : item.baseWeight();
        // (1 + itemAffinity) * categoryBoost * recency * baseWeight + 类目亲和微量加成
        double score = base * (1.0 + itemScore) * catMul * recency + catScore;
        String reason = String.format(
                Locale.ROOT,
                "itemAff=%.1f catAff=%.1f catBoost=%s recency=%.3f",
                itemScore,
                catScore,
                catMatch ? "1.5" : "1.0",
                recency);
        return new ScoredItem(item.itemId(), score, reason);
    }

    static double recencyFactor(long lastEventTs, long nowMs) {
        if (lastEventTs <= 0 || nowMs <= 0 || nowMs <= lastEventTs) {
            return 1.0;
        }
        double age = nowMs - lastEventTs;
        return Math.pow(0.5, age / (double) RECENCY_HALF_LIFE_MS);
    }
}
