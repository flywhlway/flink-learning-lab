# e12-11 · Streaming Workflow（FSM ProcessFunction）

> 对应 [ai/chapters/11-streaming-workflow.md](../../ai/chapters/11-streaming-workflow.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-11-streaming-workflow -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingWorkflowJob`

## 背景

工作流引擎擅长人工审批与长分支；流处理擅长高吞吐状态机。本章划清边界：可 checkpoint、事件驱动的短 FSM 留在 Flink，长流程外呼。

## 架构

```
Event → keyBy(user) → ValueState FSM + 处理时间空闲超时 → TRANS/STAY/TIMEOUT
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.StreamingWorkflowJob`
- 关键算子 `.uid("e12-11-…")` 与 `env.execute("e12-11-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-11-streaming-workflow \
  -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingWorkflowJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

观察 `TRANS` 沿 NEW→SEEN→READY→DONE；错误 page 出现 `STAY`；长时间无事件出现 `TIMEOUT` 回 NEW。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀（如 HIT/MISS、PASS/BLOCK、SAMPLE），便于肉眼与脚本 grep。

## 踩坑

- 用处理时间超时不等于业务 SLA（事件时间空闲更准，但本 Demo 侧重 FSM 结构）。
- 状态爆炸：每用户一状态可接受；每订单无限分支应外呼。

## 最佳实践

- 显式枚举状态与转移；拒绝隐式 if-else 泥潭。
- DONE 后可 clear 状态释放 RocksDB 压力。

## 面试题

1) Flink FSM 与 Temporal/LangGraph 的分工？2) 定时器清理为何重要？3) 如何对 FSM 做 Savepoint 升级？

见 ai/chapters/11-streaming-workflow.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e12-17-streaming-guardrail/`（Broadcast）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
