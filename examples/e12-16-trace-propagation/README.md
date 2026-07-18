# e12-16 · TraceId 全链路传播

> 对应 [ai/chapters/16-streaming-trace.md](../../ai/chapters/16-streaming-trace.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-16-trace-propagation -Dexec.mainClass=com.flywhl.flinklab.e12.TracePropagationJob`

## 背景

全链路追踪常被外包给 APM。流作业至少应保证业务 traceId 跨算子不丢，才能把 Flink 日志与上游网关对齐。

## 架构

```
Event → InjectTrace(合成 traceId) → enrich hop → sink-prep hop（同 traceId）
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.TracePropagationJob`
- 关键算子 `.uid("e12-16-…")` 与 `env.execute("e12-16-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-16-trace-propagation \
  -Dexec.mainClass=com.flywhl.flinklab.e12.TracePropagationJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

同一事件在 `enrich` 与 `sink-prep` 两行共享相同 `traceId=`。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀，便于肉眼与脚本 grep。

## 踩坑

- 只打日志不放进记录体会在旁路/侧输出丢失。
- 用处理时间当 traceId 会导致重放不一致——本 Demo 用事件 ts。

## 最佳实践

- traceId 作为记录字段一等公民；日志结构化带同一字段。
- 后续接 OTel 时做字段映射即可，不必重写拓扑。

## 面试题

1) Flink 作业如何对接 OTel？2) 侧输出如何继承 trace？3) 乱序重放对 trace 稳定性的影响？

见 ai/chapters/16-streaming-trace.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e11-async-io/`（Async I/O）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
