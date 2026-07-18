package com.flywhl.flinklab.p01.model;

/**
 * 日志富化/规则/AI 结果 POJO（落库前中间态）。
 *
 * <p>在 {@link LogEvent} 上扩展 featureJson / ruleLabel / aiRisk / aiSource；
 * 默认 aiSource=DISABLED、ruleLabel=NONE、aiRisk=NONE（D-04）。
 */
public final class LogResult {

    public String service;
    public String level;
    public String message;
    public String traceId;
    public long eventTime;
    public String featureJson;
    public String ruleLabel;
    public String aiRisk;
    public String aiSource;

    public LogResult() {
        // 无引号默认值，对齐 ClickHouseLogSink 拒引号校验（T-04-01）
        this.featureJson = "{errorCount:0}";
        this.ruleLabel = "NONE";
        this.aiRisk = "NONE";
        this.aiSource = "DISABLED";
    }

    public LogResult(LogEvent event) {
        this();
        if (event != null) {
            this.service = event.service;
            this.level = event.level;
            this.message = event.message;
            this.traceId = event.traceId;
            this.eventTime = event.eventTime;
        }
    }

    public LogResult(
            String service,
            String level,
            String message,
            String traceId,
            long eventTime,
            String featureJson,
            String ruleLabel,
            String aiRisk,
            String aiSource) {
        this.service = service;
        this.level = level;
        this.message = message;
        this.traceId = traceId;
        this.eventTime = eventTime;
        this.featureJson = featureJson != null ? featureJson : "{errorCount:0}";
        this.ruleLabel = ruleLabel != null ? ruleLabel : "NONE";
        this.aiRisk = aiRisk != null ? aiRisk : "NONE";
        this.aiSource = aiSource != null ? aiSource : "DISABLED";
    }
}
