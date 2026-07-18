# 第 05 章 · Streaming RAG:新鲜度敏感场景的检索管线

> Demo:延续 e12-04 的索引管道,新增增量索引/失效逻辑(SQL 脚本)· Level:L5

## 1. 问题:标准 RAG 的新鲜度盲区

标准 RAG(Retrieval-Augmented Generation)管线假设知识库相对静态,批量构建一次索引可以服务很长时间。但"实时性敏感"的 RAG 场景——刚发生的事故、刚更新的政策、刚变化的库存——静态索引会让 Agent 拿着过时信息做决策。Streaming RAG 要解决三个新问题:①新文档如何增量进索引(而非全量重建);②过期/被撤回的文档如何从索引失效;③检索时如何区分"权威but可能滞后"与"新鲜but未经验证"的信息源。

## 2. 架构:增量索引与失效双通道

```mermaid
flowchart TB
    NEW[(新增/更新文档流)] --> EMB[Embedding] --> UP[向量库 upsert]
    DEL[(删除/撤回事件流)] --> TOMB[墓碑标记]
    TOMB --> IDX[(向量索引)]
    UP --> IDX
    Q[查询] --> SEARCH[VECTOR_SEARCH] --> IDX
    SEARCH --> FILTER[按时间戳/置信度过滤]
    FILTER --> LLM[拼装 prompt → LLM 生成]
```

## 3. 核心设计:去重与失效

- **增量去重**:同一文档的多次更新不应在索引中留下多个版本("upsert by document_id",e09 主键表同款思路);
- **失效通道**:文档撤回/过期时,发一条"删除事件"进同一管道,写入端把该 ID 从向量库中物理删除或打墓碑标记(与 e07-C8 upsert-kafka 的墓碑消息语义一致);
- **时间戳过滤**:检索结果携带文档更新时间,下游 prompt 组装时可以按"最近 N 天"过滤,避免过时信息参与生成。

```sql
-- 失效通道:撤回事件触发向量库删除
CREATE TABLE doc_retraction_events (doc_id STRING, retracted_at TIMESTAMP(3))
WITH ('connector'='kafka','topic'='docs.retraction', ...);

-- 与 upsert 表联动:检索时排除已撤回文档(需应用层或连接器支持软删除标记)
SELECT k.*, r.retracted_at IS NOT NULL AS is_retracted
FROM milvus_docs k
LEFT JOIN doc_retraction_events r ON k.doc_id = r.doc_id;
```

## 4. 工程要点

1. **新鲜度 SLA 要显式定义**:不是所有 RAG 场景都需要秒级新鲜度,先问业务"检索结果允许滞后多久",再决定索引更新的架构复杂度(纯流式 vs 微批)。
2. **检索置信度与生成置信度分离**:向量相似度高不代表信息本身可信(可能是一篇过时或错误的文档排名很高),下游需要独立的可信度信号(如文档来源权威性、审核状态)。
3. **多源融合的时效性冲突**:当权威源(如官方政策库,更新慢但可信)与新鲜源(如实时聊天记录,更新快但未经核实)同时被检索到,prompt 组装策略需要明确优先级规则,而不是简单拼接。

## 5. Demo 状态

本章在 e12-04 索引管道基础上叠加"失效通道"逻辑,SQL 脚本随附于 `examples/e12-04-streaming-inference-vector/sql/05-retraction.sql`(与第 4 章共享 Milvus/Ollama 前置)。核心增量/失效逻辑本身是标准 Flink SQL(upsert + 软删除标记),不依赖 Preview API,置信度较高;向量库连接器细节仍受 04 章同样的版本演进限制。

## 6. 踩坑

| 坑 | 现象 | 解法 |
|---|---|---|
| 只做增量不做失效 | 索引只增不减,过时/错误信息永远可被检索到 | 失效通道与增量通道对等设计,缺一不可 |
| 检索排序只看向量相似度 | 高相似但低可信的结果排到前面 | 引入独立的可信度/权威性加权因子 |
| 新鲜源与权威源无优先级规则 | prompt 组装时信息冲突,LLM 生成前后矛盾的回答 | 显式定义信息源优先级与冲突处理策略 |

## 7. 最佳实践

- 每个 RAG 知识源在接入时登记"更新频率、失效机制、权威等级"三元组。
- 定期审计索引中"从未被检索命中"的文档比例,过高说明索引质量或路由策略有问题。

## 8. 面试题

① 为什么"只做增量不做失效"是 Streaming RAG 最常见的设计缺陷?② 向量相似度与信息可信度为什么必须分开建模?③ 如何设计一个"新鲜度 SLA"驱动的索引更新架构决策流程?

## 9. 参考资料

第 04 章(向量化与检索基础);e07-C8(upsert-kafka 墓碑消息语义的类比);ai/17(护栏——检索结果的内容安全过滤是 Streaming RAG 生产化的必要补充)。
