-- p03 车联网告警落库表（由 p03-init profile 幂等执行，不进 default init.sh）
CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts
(
    vin            String,
    alert_type     String,              -- MATCH | TIMEOUT
    pattern_id     String DEFAULT '',   -- HARSH_THEN_FAULT | TRIPLE_HARSH | DTC_PAIR（D-08）
    signal_summary String,
    harsh_value    Float64,
    fault_value    Float64,
    event_time     DateTime64(3),
    ingest_time    DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, vin);

-- 兼容已有表：幂等加列（ClickHouse 24.x ADD COLUMN IF NOT EXISTS）
ALTER TABLE flinklab.vehicle_alerts
    ADD COLUMN IF NOT EXISTS pattern_id String DEFAULT '';
