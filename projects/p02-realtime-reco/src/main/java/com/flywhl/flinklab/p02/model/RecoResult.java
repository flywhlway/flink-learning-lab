package com.flywhl.flinklab.p02.model;

/**
 * 推荐结果 POJO（落库/发 Kafka 前中间态，D-06）。
 *
 * <p>默认 {@code featureSource=STATE_ONLY}，供后续 Sink 白名单校验。
 */
public final class RecoResult {

    public String userId;
    public String itemId;
    public double score;
    public long eventTimeMs;
    public String reason;
    public String featureSource;
    public String featureSnapshot;

    public RecoResult() {
        this.reason = "";
        this.featureSource = "STATE_ONLY";
        this.featureSnapshot = "{}";
    }

    public RecoResult(
            String userId,
            String itemId,
            double score,
            long eventTimeMs,
            String reason,
            String featureSource,
            String featureSnapshot) {
        this.userId = userId;
        this.itemId = itemId;
        this.score = score;
        this.eventTimeMs = eventTimeMs;
        this.reason = reason != null ? reason : "";
        this.featureSource = featureSource != null ? featureSource : "STATE_ONLY";
        this.featureSnapshot = featureSnapshot != null ? featureSnapshot : "{}";
    }

    public String userId() {
        return userId;
    }

    public String itemId() {
        return itemId;
    }

    public double score() {
        return score;
    }

    public long eventTimeMs() {
        return eventTimeMs;
    }

    public String reason() {
        return reason;
    }

    public String featureSource() {
        return featureSource;
    }

    public String featureSnapshot() {
        return featureSnapshot;
    }
}
