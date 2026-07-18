-- 兼容 Phase 1 已有表：幂等加列（ClickHouse 24.x ADD COLUMN IF NOT EXISTS）
ALTER TABLE flinklab.vehicle_alerts
    ADD COLUMN IF NOT EXISTS pattern_id String DEFAULT ''
