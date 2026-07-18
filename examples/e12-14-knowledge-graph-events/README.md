# e12-14 · Knowledge Graph 事件三元组（MapState）

> 对应 [ai/chapters/14-streaming-knowledge-graph.md](../../ai/chapters/14-streaming-knowledge-graph.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-14-knowledge-graph-events -Dexec.mainClass=com.flywhl.flinklab.e12.KnowledgeGraphEventsJob`

## 背景

知识图谱在流场景里首先是「边的增量 upsert」。本 Demo 用 MapState 存 (predicate→weight)，把图写入降到可 checkpoint 的状态操作。

## 架构

```
Event → keyBy(user) → MapState 三元组 upsert → INSERT/UPDATE
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.KnowledgeGraphEventsJob`
- 关键算子 `.uid("e12-14-…")` 与 `env.execute("e12-14-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-14-knowledge-graph-events \
  -Dexec.mainClass=com.flywhl.flinklab.e12.KnowledgeGraphEventsJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

同一 user+page 首次 `INSERT`，再次同边 `UPDATE` 且 weight 累加。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀，便于肉眼与脚本 grep。

## 踩坑

- MapState key 设计要稳定（谓词+客体）；随意拼接难做查询规划。
- 全图全局查询不适合 Keyed 分区——需要另建索引作业。

## 最佳实践

- 先事件三元组，再考虑图库同步侧输出。
- 权重更新可带事件时间，便于过期边淘汰。

## 面试题

1) 流式 KG 与批式 ETL 图构建差异？2) MapState vs ListState 存边？3) 如何导出到外部图库且 exactly-once？

见 ai/chapters/14-streaming-knowledge-graph.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e11-async-io/`（Async I/O）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
