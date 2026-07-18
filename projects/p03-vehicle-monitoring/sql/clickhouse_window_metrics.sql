-- p03 车联网窗口聚合指标表（由 p03-init profile 第三次 POST 幂等执行，不进 default init.sh）
-- 注意：ClickHouse HTTP 默认禁多语句；本文件仅含 CREATE，禁止与 alerts DDL 合并。
CREATE TABLE IF NOT EXISTS flinklab.vehicle_window_metrics
(
    vin          String,
    window_start DateTime64(3),
    window_end   DateTime64(3),
    event_count  UInt64,
    harsh_count  UInt64,
    dtc_count    UInt64,
    ingest_time  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (window_start, vin)
