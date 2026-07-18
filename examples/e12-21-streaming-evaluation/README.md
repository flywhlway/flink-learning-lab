# e12-21 · Streaming Evaluation（窗口准确率/延迟）

> 对应 [ai/chapters/21-streaming-evaluation.md](../../ai/chapters/21-streaming-evaluation.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-21-streaming-evaluation -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingEvaluationJob`

## 背景

模型质量不能只靠离线集。流式评测把准确率/延迟做成窗口指标，才能对接反馈闭环与告警。

## 架构

```
Event(+WM) → keyBy → TumblingEventTime 5s → Aggregate(正确数/总数/延迟) → EVAL
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.StreamingEvaluationJob`
- 关键算子 `.uid("e12-21-…")` 与 `env.execute("e12-21-…")`
- 包名统一 `com.flywhl.flinklab.e12`

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-21-streaming-evaluation \
  -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingEvaluationJob
```

## 验证

周期性出现 `EVAL` 行，含 accuracy 与 avgLatencyMs；n 为窗口内样本数。

## 源码讲解

关键路径：无界事件进入 → 业务算子 → `print()`。教学断言体现在输出前缀。

## 踩坑

- 用处理时间延迟当评测会混入反压噪声——本 Demo 已标明是模拟。
- 标签如何对齐预测是生产难题（本 Demo 用奇偶简化）。

## 最佳实践

- 评测指标进侧输出/ClickHouse，与业务主链路解耦。
- 窗口大小与反馈 SLA 对齐。

## 面试题

1) 在线评测与离线评测如何互补？2) 标签延迟到达怎么处理？3) 准确率暴跌是否应自动熔断路由？

## 参考

- `examples/e12-17-streaming-guardrail/`、`examples/e12-18-streaming-cost-control/`
- 版本 SSOT：根 README + `examples/pom.xml`（Flink 2.2.1 / JDK 21）
