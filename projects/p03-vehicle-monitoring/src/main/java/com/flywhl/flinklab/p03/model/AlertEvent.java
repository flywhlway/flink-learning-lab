package com.flywhl.flinklab.p03.model;

/**
 * CEP 告警输出 POJO（MATCH 主流 / TIMEOUT Side Output 共用）。
 *
 * <p>字段对齐 RESEARCH Open Questions RESOLVED：同表 {@code alert_type}；
 * {@code patternId} 供 Broadcast 出口门控与 ClickHouse {@code pattern_id}（D-08/D-09）。
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
     * 触发本告警的模式 ID（门控过滤 / CH 断言用）。
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
            String message,
            String patternId) {
        this.vin = vin;
        this.alertType = alertType;
        this.harshValue = harshValue;
        this.faultValue = faultValue;
        this.eventTime = eventTime;
        this.message = message;
        this.patternId = patternId;
    }
}
