-- p01 日志富化/规则/AI 落库表（由 p01-init profile 幂等执行，不进 default init.sh）
-- 注意：ClickHouse HTTP 默认禁多语句；本文件仅含单条 CREATE，供 wget --post-file 一次 POST。
CREATE TABLE IF NOT EXISTS flinklab.log_results
(
    service      String,
    level        String,
    message      String,
    trace_id     String,
    event_time   DateTime64(3),
    feature_json String,
    rule_label   String,
    ai_risk      String,
    ai_source    String,
    ingest_time  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, service, trace_id)
