-- e12-04/e12-05 · 失效通道:文档撤回事件触发向量库删除(ai/05 第 3 节)。
-- 增量索引(02 脚本)与失效通道(本脚本)对等设计,缺一不可。

CREATE TABLE doc_retraction_events (
    ticket_id STRING,
    retracted_at TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '0.1',           -- 低频撤回事件
    'fields.ticket_id.length' = '8'
);

-- 软删除标记方案:检索侧 LEFT JOIN 排除已撤回工单
-- (物理删除方案依赖连接器 DELETE 支持;软删除是普适降级方案,墓碑语义同 e07-C8)
SELECT k.ticket_id, k.text,
       r.retracted_at IS NOT NULL AS is_retracted
FROM milvus_tickets k
LEFT JOIN doc_retraction_events r ON k.ticket_id = r.ticket_id;

-- 生产版本:撤回事件直接驱动 Milvus delete(Async I/O 调 SDK),并在
-- 检索链路上增加 is_retracted 过滤,双保险。
