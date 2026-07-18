package com.flywhl.flinklab.p02.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 随流携带的会话/近窗特征快照（D-01）：供 Redis 写与规则打分降级（STATE_ONLY）。
 *
 * <p>公开字段 + 无参构造满足 Flink POJO；访问器兼容 Wave 0 {@code RuleScorerTest}。
 */
public final class FeatureSnapshot {

    public String userId;
    public Map<String, Double> categoryAffinity;
    public Map<String, Double> itemAffinity;
    public long lastEventTs;
    public long clickCount;

    public FeatureSnapshot() {
        this.userId = "";
        this.categoryAffinity = new HashMap<>();
        this.itemAffinity = new HashMap<>();
        this.lastEventTs = 0L;
        this.clickCount = 0L;
    }

    public FeatureSnapshot(
            String userId,
            Map<String, Double> categoryAffinity,
            Map<String, Double> itemAffinity,
            long lastEventTs) {
        this(userId, categoryAffinity, itemAffinity, lastEventTs, 0L);
    }

    public FeatureSnapshot(
            String userId,
            Map<String, Double> categoryAffinity,
            Map<String, Double> itemAffinity,
            long lastEventTs,
            long clickCount) {
        this.userId = userId != null ? userId : "";
        this.categoryAffinity = copyMap(categoryAffinity);
        this.itemAffinity = copyMap(itemAffinity);
        this.lastEventTs = lastEventTs;
        this.clickCount = clickCount;
    }

    public String userId() {
        return userId;
    }

    public Map<String, Double> categoryAffinity() {
        return categoryAffinity;
    }

    public Map<String, Double> itemAffinity() {
        return itemAffinity;
    }

    public long lastEventTs() {
        return lastEventTs;
    }

    public long clickCount() {
        return clickCount;
    }

    /** 紧凑摘要（无引号），供 CH feature_snapshot 列与 reason 旁路。 */
    public String compactSummary() {
        return "{user="
                + userId
                + ",clicks="
                + clickCount
                + ",cats="
                + categoryAffinity.size()
                + ",items="
                + itemAffinity.size()
                + ",ts="
                + lastEventTs
                + "}";
    }

    private static Map<String, Double> copyMap(Map<String, Double> src) {
        if (src == null || src.isEmpty()) {
            return new HashMap<>();
        }
        return new HashMap<>(src);
    }

    public static Map<String, Double> unmodifiableView(Map<String, Double> src) {
        return Collections.unmodifiableMap(copyMap(src));
    }
}
