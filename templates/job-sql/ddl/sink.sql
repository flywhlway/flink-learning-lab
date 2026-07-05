-- 输出表 DDL 示例(print 占位,替换为真实落地目标)。
-- 命名建议:dwd_/dws_/ads_<业务域>_<表名>,主键声明决定 upsert 语义(e07-C2)。
CREATE TABLE IF NOT EXISTS ads_events_summary (
    page STRING,
    pv   BIGINT
    -- 替换点(项目方): 若下游需要 upsert 语义,取消下行注释并声明主键
    -- , PRIMARY KEY (page) NOT ENFORCED
) WITH (
    -- 替换点(项目方): 替换为真实 connector(upsert-kafka/jdbc/paimon 等)
    'connector' = 'print'
);
