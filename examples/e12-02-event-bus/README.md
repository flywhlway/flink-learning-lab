# e12-02 · 事件契约与消费者组隔离

> 对应 [ai/chapters/02-event-bus.md](../../ai/chapters/02-event-bus.md) · Level:L1
> 运行:`mvn -q -Plocal compile exec:java -pl e12-02-event-bus -Dexec.mainClass=com.flywhl.flinklab.e12.EventBusTopicDesignJob`

## 背景

本地模拟版的事件总线设计:用 DataStream 的多路 fan-out 模拟 Kafka 消费者组的广播语义,重点讲清"命令 vs 事实"两种事件契约的产出差异,而不需要真正起 Kafka 集群。生产版本(用真实 KafkaSource + 不同 group.id)见 ai/02 第 4 节代码片段。

## 验证方式

观察三种前缀输出:`[风控 Agent]` 与 `[监控大屏]` 都完整收到同一份事实流(各自独立过滤条件),证明"广播语义"生效;`[单一执行者]` 只在触发命令时出现,体现命令类事件"不应被多方重复执行"的语义差异。

## 源码要点

- `RiskSignalFactV1`/`DispatchAlertCommandV1` 是 ai/02 第 3 节"事件契约"的代码化,`schemaVersion` 字段是版本治理的最小实践。
- 两个"消费者组"在本 Demo 里只是两条独立的下游处理链(`.filter().map().print()`),生产环境替换为两个 `group.id` 不同的 `KafkaSource` 即完全等价。

## 面试题

见 ai/chapters/02-event-bus.md 第 7 节。
