# e12-13 · Flink × LangGraph Mock（AsyncIO 超时降级）

> 对应 [ai/chapters/13-flink-langgraph.md](../../ai/chapters/13-flink-langgraph.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-13-langgraph-mock -Dexec.mainClass=com.flywhl.flinklab.e12.LangGraphMockJob`

## 背景

LangGraph 适合复杂图编排；Flink 适合高吞吐与状态。边界原则：可外呼，但必须超时降级，避免外呼拖垮作业。

## 架构

```
Event → AsyncDataStream.unorderedWait(Mock HTTP) → REMOTE 或 timeout→LOCAL
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.LangGraphMockJob`
- 关键算子 `.uid("e12-13-…")` 与 `env.execute("e12-13-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-13-langgraph-mock \
  -Dexec.mainClass=com.flywhl.flinklab.e12.LangGraphMockJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

输出混有 `REMOTE`（成功外呼）与 `LOCAL`（超时降级）；作业不应因 mock 慢请求失败。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀，便于肉眼与脚本 grep。

## 踩坑

- 有序 wait 会放大队头阻塞；教学用 unordered。
- timeout 回调里再抛异常会导致作业失败——必须降级完成。

## 最佳实践

- 外呼预算（超时/并发）与业务 SLA 对齐。
- 降级路径要可观测（前缀 LOCAL）。

## 面试题

1) 何时该外呼 LangGraph 而非流内 FSM？2) Async I/O 容量与反压？3) 与 e11-C2 三件套如何对照？

见 ai/chapters/13-flink-langgraph.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e11-async-io/`（Async I/O）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
