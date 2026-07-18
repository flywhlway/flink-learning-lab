package com.flywhl.flinklab.p01.rule;

import com.flywhl.flinklab.p01.model.LogEvent;
import com.flywhl.flinklab.p01.model.LogResult;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 规则标签：AUTH_FAIL / ERROR_BURST / NONE（可单测纯逻辑，LOG-02）。
 *
 * <p>作业侧以 {@link MapFunction} 写入 {@link LogResult#ruleLabel}；
 * AI off 时保持 {@code ai_source=DISABLED}、{@code ai_risk=NONE}（D-04）。
 */
public final class RuleTagger implements MapFunction<LogResult, LogResult> {

    /** 特征 ERROR 计数达到此阈值打 ERROR_BURST。 */
    public static final long ERROR_BURST_THRESHOLD = 5L;

    @Override
    public LogResult map(LogResult value) {
        if (value == null) {
            return null;
        }
        value.ruleLabel = tag(value);
        // 本切片默认 AI off：强制权威列
        if (value.aiSource == null || value.aiSource.isBlank()) {
            value.aiSource = "DISABLED";
        }
        if (value.aiRisk == null || value.aiRisk.isBlank()) {
            value.aiRisk = "NONE";
        }
        return value;
    }

    /**
     * 单测入口：仅看事件字段（AUTH_FAIL / NONE）。
     */
    public static String tag(LogEvent event) {
        if (event == null) {
            return "NONE";
        }
        return tagAuthOrNone(event.service, event.level, event.message);
    }

    /**
     * 作业入口：AUTH_FAIL 优先，其次特征阈值 ERROR_BURST，否则 NONE。
     */
    public static String tag(LogResult result) {
        if (result == null) {
            return "NONE";
        }
        String authOrNone = tagAuthOrNone(result.service, result.level, result.message);
        if ("AUTH_FAIL".equals(authOrNone)) {
            return "AUTH_FAIL";
        }
        if (errorCount(result.featureJson) >= ERROR_BURST_THRESHOLD) {
            return "ERROR_BURST";
        }
        return authOrNone;
    }

    static String tagAuthOrNone(String service, String level, String message) {
        if (level == null || !"ERROR".equalsIgnoreCase(level)) {
            return "NONE";
        }
        String svc = service == null ? "" : service.toLowerCase();
        String msg = message == null ? "" : message.toLowerCase();
        boolean authService = svc.contains("auth");
        boolean authMessage = msg.contains("authentication failed")
                || msg.contains("auth failed")
                || msg.contains("login failed")
                || msg.contains("unauthorized");
        if (authService || authMessage) {
            return "AUTH_FAIL";
        }
        return "NONE";
    }

    static long errorCount(String featureJson) {
        if (featureJson == null || featureJson.isBlank()) {
            return 0L;
        }
        // 轻量解析：{errorCount:N,...}（与 FeatureEnricher 紧凑格式对齐）
        String key = "errorCount:";
        int idx = featureJson.indexOf(key);
        if (idx < 0) {
            return 0L;
        }
        int start = idx + key.length();
        int end = start;
        while (end < featureJson.length() && Character.isDigit(featureJson.charAt(end))) {
            end++;
        }
        if (end == start) {
            return 0L;
        }
        try {
            return Long.parseLong(featureJson.substring(start, end));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
