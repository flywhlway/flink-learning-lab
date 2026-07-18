# e12-12 · Multi-Agent 协作拓扑（双流 connect）

> 对应 [ai/chapters/12-streaming-multi-agent.md](../../ai/chapters/12-streaming-multi-agent.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-12-multi-agent-topology -Dexec.mainClass=com.flywhl.flinklab.e12.MultiAgentTopologyJob`

## 背景

多 Agent 协作常被画成服务网格。流式拓扑里，协作 = 同 key 上状态可见。本 Demo 用感知/决策双流 connect，避免引入 Agents Preview。

## 架构

```
Sense 流 ──┐
           ├─ keyBy(user) connect → CollaborateFn(状态)
Decide 流 ─┘
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.MultiAgentTopologyJob`
- 关键算子 `.uid("e12-12-…")` 与 `env.execute("e12-12-…")`，便于 Savepoint / 观测对齐
- 包名统一 `com.flywhl.flinklab.e12`，与既有 e12-01/17/22 一致

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-12-multi-agent-topology \
  -Dexec.mainClass=com.flywhl.flinklab.e12.MultiAgentTopologyJob
```

本地 profile 将 `provided` 作用域的 Flink 依赖提升为可执行 classpath；集群提交仍走 shade jar + `flink run`。

## 验证

先出现若干 `SENSE`；随后 `DECIDE` 带 basedOn；若决策先到则 `WAIT`。

## 源码讲解

关键路径：无界事件进入 → 按业务 key 分区 → 状态算子完成教学点逻辑 → `print()` 观察结果。
所有教学断言都体现在输出前缀，便于肉眼与脚本 grep。

## 踩坑

- 两流 watermark/速率差会导致长时间 WAIT——生产要超时旁路。
- 不要用广播全局变量冒充 Agent 共享内存。

## 最佳实践

- 角色拆流 + 共享业务 key；状态只存协作契约。
- 输出带角色前缀，便于观测与回放。

## 面试题

1) CoProcess 与 Broadcast 选哪个做协作？2) 多 Agent 失败隔离怎么设计？3) 与 e12-11 FSM 如何组合？

见 ai/chapters/12-streaming-multi-agent.md 对应小节；本 README 只固化与本 Demo 可对照的考点。

## 参考

- 仓库内：`examples/e12-01-polling-vs-event/`（零依赖骨架）、`examples/e11-async-io/`（Async I/O）
- 版本 SSOT：根 README 版本矩阵 + `examples/pom.xml` 属性区（Flink 2.2.1 / JDK 21）
