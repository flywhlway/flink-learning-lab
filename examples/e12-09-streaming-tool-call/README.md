# e12-09 · Streaming Tool Call（幂等键 + 副作用侧输出）

> 对应 [ai/chapters/09-streaming-tool-call.md](../../ai/chapters/09-streaming-tool-call.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-09-streaming-tool-call -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingToolCallJob`

## 背景

Tool Call 的核心风险是副作用重复：checkpoint 重放或至少一次投递会导致重复发券。本章降级路径不依赖 Agents Reconciler，用幂等键 ValueState + Side Output 演示「动作可审计、重放不重做」。

## 架构

```
Event → keyBy(user|page) → ValueState 幂等 → 主流通审计 / Side Output 副作用
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.StreamingToolCallJob`
- 关键算子 `.uid("e12-09-…")` 与 `env.execute("e12-09-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-09-streaming-tool-call \
  -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingToolCallJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

同一 user+page 首次高金额出现 `OK` + `SIDE-EFFECT`；后续同键出现 `SKIP`。低金额出现 `HOLD`。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀（如 HIT/MISS、PASS/BLOCK、SAMPLE），便于肉眼与脚本 grep。

## 踩坑

- 幂等键粒度选错（过粗吞掉合法重试，过细仍重复）。
- 把副作用写在主流通而不用侧输出，难与业务审计解耦。

## 最佳实践

- 副作用与审计分流；状态只记「是否已做」。
- 生产把 Side Output 接到 Kafka/工单系统，仍保持幂等键。

## 面试题

1) 幂等键与 exactly-once sink 的关系？2) Side Output 适合承载哪些副作用？3) Agents Reconciler 相对本降级路径多解决了什么？

见 ai/chapters/09-streaming-tool-call.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e12-17-streaming-guardrail/`（Broadcast）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
