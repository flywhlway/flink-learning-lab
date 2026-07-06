-- e12-04 · 第三步:流内向量检索(VECTOR_SEARCH,Flink 2.2+)。
-- 新工单一到即查最相似的 5 条历史工单——"刚发生的事立刻可被检索"的能力验证。

SELECT q.ticket_id, r.ticket_id AS similar_ticket, r.score
FROM tickets_with_vector AS q,
     LATERAL TABLE (VECTOR_SEARCH(TABLE milvus_tickets, DESCRIPTOR(embedding), q.embedding, 5)) AS r
WHERE r.score > 0.6;   -- 最低相似度阈值(ai/04 要点 3:top_k 不代表足够相似)
