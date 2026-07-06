-- e12-04 · 第二步:流式向量化 + 写入 Milvus(upsert 语义)。
-- ⚠️ Flink 官方 Milvus 连接器仍在快速演进,WITH 参数为示意写法;
--    若连接器不可用,降级路径:e11 Async I/O 直接调用 Milvus Java SDK。

CREATE TABLE tickets_stream (
    ticket_id STRING,
    text STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.ticket_id.length' = '8',
    'fields.text.length' = '30'
);

-- 向量化视图:每条工单文本 → embedding 向量
CREATE VIEW tickets_with_vector AS
SELECT t.ticket_id, t.text, e.embedding
FROM tickets_stream AS t,
     LATERAL TABLE (ML_PREDICT(TABLE t, MODEL text_embedder, DESCRIPTOR(text))) AS e;

-- Milvus 向量表(主键 = upsert 语义:同 ticket_id 更新即覆盖旧向量,ai/04 要点 2)
CREATE TABLE milvus_tickets (
    ticket_id STRING,
    text STRING,
    embedding ARRAY<FLOAT>,
    PRIMARY KEY (ticket_id) NOT ENFORCED
) WITH (
    'connector' = 'milvus',
    'host' = 'milvus-standalone',        -- 容器内互访;宿主机侧为 localhost:19530
    'port' = '19530',
    'collection' = 'tickets',
    'vector-field' = 'embedding'
);

INSERT INTO milvus_tickets SELECT ticket_id, text, embedding FROM tickets_with_vector;
