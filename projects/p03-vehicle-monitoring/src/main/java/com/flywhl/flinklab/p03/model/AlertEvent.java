package com.flywhl.flinklab.p03.model;

/**
 * CEP 告警输出 POJO（MATCH 主流 / TIMEOUT Side Output 共用）。
 *
 * <p>字段对齐 RESEARCH Open Questions RESOLVED：同表 {@code alert_type}。
 */
public final class AlertEvent {

    public String vin;
    /** MATCH 或 TIMEOUT */
    public String alertType;
    public double harshValue;
    public double faultValue;
    public long eventTime;
    public String message;
    /**
     * 触发本告警的模式 ID（门控过滤用；完整 Handler/DDL 贯通属 02-02b）。
     */
    public String patternId;

    public AlertEvent() {
    }

    public AlertEvent(
            String vin,
            String alertType,
            double harshValue,
            double faultValue,
            long eventTime,
            String message) {
        this.vin = vin;
        this.alertType = alertType;
        this.harshValue = harshValue;
        this.faultValue = faultValue;
        this.eventTime = eventTime;
        this.message = message;
    }
}
