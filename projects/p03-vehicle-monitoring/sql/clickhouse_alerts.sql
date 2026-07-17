-- p03 车联网告警落库表（由 p03-init profile 幂等执行，不进 default init.sh）
CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts
(
    vin            String,
    alert_type     String,              -- MATCH | TIMEOUT
    signal_summary String,
    harsh_value    Float64,
    fault_value    Float64,
    event_time     DateTime64(3),
    ingest_time    DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, vin);
