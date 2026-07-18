# e12-23 · Online Learning 样本侧输出

> 对应 [ai/chapters/23-streaming-online-learning.md](../../ai/chapters/23-streaming-online-learning.md) · Level:L2–L3
> 运行:`mvn -q -Plocal compile exec:java -pl e12-23-online-learning-sample -Dexec.mainClass=com.flywhl.flinklab.e12.OnlineLearningSampleJob`

## 背景

在线学习不是在 Flink TaskManager 里训大模型，而是可靠地产出训练样本。本 Demo 用侧输出模拟样本通道，明确 Flink×Ray 分工。

## 架构

```
Event → keyBy → 打分(主流) + 显著变化时 Side Output 样本
```

本 Demo **零外部依赖**：源为 `Labs.events` / datagen，状态在 Flink Keyed/Broadcast State 内完成，不引入 Milvus、Ollama、flink-agents Preview 坐标，保证进主 `examples/pom.xml` 聚合构建可编译。

## 代码锚点

- 主类：`com.flywhl.flinklab.e12.OnlineLearningSampleJob`
- 关键算子 `.uid("e12-23-…")` 与 `env.execute("e12-23-…")`
- 包名统一 `com.flywhl.flinklab.e12`

## 启动

```bash
cd examples
mvn -q -Plocal compile exec:java -pl e12-23-online-learning-sample \
  -Dexec.mainClass=com.flywhl.flinklab.e12.OnlineLearningSampleJob
```

## 验证

主流持续 `SCORE`；金额跳变时出现 `SAMPLE` 行含 features 与 label。

## 源码讲解

关键路径：无界事件进入 → 业务算子 → `print()`。教学断言体现在输出前缀。

## 踩坑

- 在 TM 内直接调训练框架会破坏反压与运维边界。
- 样本无 schema/版本字段会导致训练侧不可复现。

## 最佳实践

- 样本侧输出带特征版本与事件时间。
- 训练集群消费侧输出 topic，Flink 只保证样本语义。

## 面试题

1) 为何不在 Flink 里直接 SGD？2) 样本倾斜如何采样？3) 特征版本与模型版本如何对齐？

## 参考

- `examples/e12-17-streaming-guardrail/`、`examples/e12-18-streaming-cost-control/`
- 版本 SSOT：根 README + `examples/pom.xml`（Flink 2.2.1 / JDK 21）
