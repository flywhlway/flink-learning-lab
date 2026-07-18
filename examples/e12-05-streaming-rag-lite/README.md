# e12-05 · Streaming RAG Lite（Keyed State 片段索引）

> 对应 [ai/chapters/05-streaming-rag.md](../../ai/chapters/05-streaming-rag.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-05-streaming-rag-lite -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingRagLiteJob`

## 背景

RAG 管线常被写成「embedding → 向量库 → 重排」。本章强调新鲜度敏感检索：文档片段随事件到达即入库，查询时优先用本地状态索引。本 Demo 用 Keyed ListState 模拟片段索引，证明无 Milvus 也能讲清流式 RAG 的状态路径。

## 架构

```
Event 流 → keyBy(userId) → ListState 片段索引 → amount 超阈触发规则检索 → HIT/MISS
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.StreamingRagLiteJob`
- 关键算子 `.uid("e12-05-…")` 与 `env.execute("e12-05-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-05-streaming-rag-lite \
  -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingRagLiteJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

观察输出前缀 `RAG-HIT` / `RAG-MISS`：高金额事件触发检索；同 page 片段会命中；新用户早期索引为空时出现 MISS。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀（如 HIT/MISS、PASS/BLOCK、SAMPLE），便于肉眼与脚本 grep。

## 踩坑

- ListState 无界增长会拖垮状态后端：本 Demo 强制 MAX_CHUNKS=8，生产要用 TTL/窗口淘汰。
- 规则检索 ≠ 向量检索：本 Demo 故意用字符串包含匹配，避免引入未登记坐标。

## 最佳实践

- 先落地「可 checkpoint 的索引更新」，再接向量库作召回增强。
- uid 固定，便于后续把检索算子替换为 AsyncIO 外呼而不丢状态血缘。

## 面试题

1) 流式 RAG 与批式离线索引的新鲜度差异？2) Keyed State 片段索引的容量治理？3) 何时必须上向量库？

见 ai/chapters/05-streaming-rag.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e12-17-streaming-guardrail/`（Broadcast）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
