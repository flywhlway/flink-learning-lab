# e12-20 · Embedding Cache（LRU MapState + 命中率）

> 对应 [ai/chapters/20-streaming-embedding-cache.md](../../ai/chapters/20-streaming-embedding-cache.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-20-embedding-cache -Dexec.mainClass=com.flywhl.flinklab.e12.EmbeddingCacheJob`

## 背景

重复查询同一文本时，embedding 调用昂贵。本地 LRU 缓存是第一道防线；本 Demo 用 MapState 教学命中率与容量治理，不接真实模型。

## 架构

```
Event → keyBy(user) → MapState LRU(page→ts) → HIT/MISS + hitRate
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.EmbeddingCacheJob`
- 关键算子 `.uid("e12-20-…")` 与 `env.execute("e12-20-…")`
- 包名统一 `com.flywhl.flinklab.e12`

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-20-embedding-cache \
  -Dexec.mainClass=com.flywhl.flinklab.e12.EmbeddingCacheJob
```

## 验证

同一 user 重复 page 先 `MISS` 后 `HIT`；命中率逐步上升；容量满时淘汰旧页。

## 源码讲解

关键路径：无界事件进入 → 业务算子 → `print()`。教学断言体现在输出前缀。

## 踩坑

- 按 user 分区的缓存不共享跨用户语义——全局缓存需另拓扑。
- 只用处理顺序 LRU，未做 TTL；生产要双维度淘汰。

## 最佳实践

- 命中率作为自定义指标上报（本 Demo 打在日志）。
- 与 e11-C3 组合：本地 Miss 再 Async 外呼。

## 面试题

1) 语义缓存键如何设计（原文 vs hash）？2) 缓存与 embedding 模型版本绑定？3) 命中率多少才值得上 Redis？

## 参考

- `examples/e12-17-streaming-guardrail/`、`examples/e12-18-streaming-cost-control/`
- 版本 SSOT：根 README + `examples/pom.xml`（Flink 2.2.1 / JDK 21）
