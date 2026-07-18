-- p02 实时推荐结果权威表（由 p02-init profile 幂等执行，不进 default init.sh）
-- 注意：ClickHouse HTTP 默认禁多语句；本文件仅含单条 CREATE，供 wget --post-file 一次 POST。
CREATE TABLE IF NOT EXISTS flinklab.reco_results
(
    user_id          String,
    item_id          String,
    score            Float64,
    event_time       DateTime64(3),
    reason           String,
    feature_source   String,
    feature_snapshot String,
    ingest_time      DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, user_id, item_id)
