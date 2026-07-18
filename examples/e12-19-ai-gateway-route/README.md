# e12-19 · AI Gateway Broadcast 路由

> 对应 [ai/chapters/19-streaming-ai-gateway.md](../../ai/chapters/19-streaming-ai-gateway.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-19-ai-gateway-route -Dexec.mainClass=com.flywhl.flinklab.e12.AiGatewayRouteJob`

## 背景

AI Gateway 的核心能力是按租户/成本/灰度把请求路由到不同模型，且规则可热更新。Broadcast State 正是该模式的 Flink 原生解。

## 架构

```
请求流 keyBy(user) ──┐
                     ├─ KeyedBroadcastProcess → ROUTE
路由规则 Broadcast ──┘
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.AiGatewayRouteJob`
- 关键算子 `.uid("e12-19-…")` 与 `env.execute("e12-19-…")`
- 包名统一 `com.flywhl.flinklab.e12`

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-19-ai-gateway-route \
  -Dexec.mainClass=com.flywhl.flinklab.e12.AiGatewayRouteJob
```

## 验证

先见 `RULE` 更新；随后 `ROUTE` 行按 user 前缀落到 cheap/premium/default-local。

## 源码讲解

关键路径：无界事件进入 → 业务算子 → `print()`。教学断言体现在输出前缀。

## 踩坑

- 路由规则全量进 Broadcast 要注意状态大小。
- 默认模型必须存在，避免空路由丢请求。

## 最佳实践

- 规则带版本号；观测 ROUTE 分布做成本归因。
- 与 e12-17 护栏可串联：先路由再护栏。

## 面试题

1) Broadcast vs Kafka 配置中心？2) 网关层限流放哪？3) 如何做模型灰度百分比路由？

## 参考

- `examples/e12-17-streaming-guardrail/`、`examples/e12-18-streaming-cost-control/`
- 版本 SSOT：根 README + `examples/pom.xml`（Flink 2.2.1 / JDK 21）
