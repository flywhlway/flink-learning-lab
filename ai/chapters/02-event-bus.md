# 第 02 章 · Streaming Event Bus:Agent 的神经系统

> Demo:e12-02(Kafka 主题设计 + 事件 Schema 治理,纯 DataStream/SQL,无外部依赖)· Level:L1

## 1. 问题:Agent 之间如何"神经传导"

一个成熟的事件驱动 AI 平台里,事件不是只流向一个 Agent,而是在多个 Agent、多个下游系统之间广播、路由、聚合。这条"神经系统"设计得好坏,直接决定平台能否演进(新增一个 Agent 消费某类事件不应牵动全局)、能否追溯(出问题时能否重放事件序列复现现场)、能否治理(Schema 变更不搞崩下游)。

## 2. 主题设计的四个决策

```mermaid
flowchart TD
    D1{事件是命令还是事实?} -->|命令 command\n如 SendAlertCommand| C1[单一消费者语义\n消费即执行,不广播]
    D1 -->|事实 fact\n如 PaymentFailedEvent| C2[多消费者广播语义\n多个 Agent 各自订阅]
    D2{事件粒度} -->|细粒度原始事件| G1[ods 层 topic\n如 events.raw.payment]
    D2 -->|聚合后的业务事件| G2[dwd/dws 层 topic\n如 events.enriched.risk-signal]
    D3{Schema 演进策略} --> SC[Schema Registry\n向后兼容:只加字段不删字段]
    D4{分区键选择} --> PK[按业务实体 ID\n保证同一实体的事件序不乱]
```

**命令 vs 事实**是最容易被忽视的一条:命令类事件("请发送这条告警")语义上只应被处理一次,事实类事件(“支付失败了”)天然允许多个 Agent 各自订阅、各自决策。把两者混在同一个 topic 里,会导致"到底该谁处理这条消息"的责任不清。

## 3. 事件契约(Event Contract)设计

每个事件类型应固化一份契约,包含:事件类型名、版本号、schema(字段+类型+是否可空)、分区键字段、产生该事件的系统、消费该事件的已知 Agent 清单。这份契约本质上是 02-02 讲的 04 章"Runtime 序列化"红线(POJO/Avro 可演进,禁 Kryo)在事件总线层面的延伸——**事件契约就是分布式系统里的 API 契约**,理应像 REST API 一样有版本治理流程。

```java
// e12-02 事件契约示例:版本化的车联网信号事件
public class VehicleSignalEventV1 {
    public String vin;            // 分区键
    public String signalType;     // ENUM 语义,但传输用 String 便于演进
    public double value;
    public long eventTimeMs;
    public String schemaVersion = "v1";   // 契约显式携带版本,消费方按版本分支处理
}
```

## 4. Demo:主题设计与消费者组隔离(e12-02)

演示三个 topic(`events.raw.vehicle-signal` 原始信号、`events.enriched.risk-signal` 富化后的风险信号、`commands.alert.dispatch` 告警下发命令)之间的生产消费关系,以及**消费者组隔离**的实践:同一个事实类 topic 被两个独立消费者组(风控 Agent、监控大屏)各自完整消费一遍,互不影响进度——这是 Kafka 消费者组模型的核心价值,也是"事实广播"语义在基础设施层面的实现方式。

```java
// 两个独立消费者组各自订阅同一 topic,分别用于风控决策与监控大屏
// group.id 不同即互不干扰进度,这是"广播语义"的物理实现
KafkaSource<String> riskConsumer = KafkaSource.<String>builder()
        .setTopics("events.enriched.risk-signal")
        .setGroupId("agent-risk-control")      // 独立消费者组 1
        .build();
KafkaSource<String> dashboardConsumer = KafkaSource.<String>builder()
        .setTopics("events.enriched.risk-signal")
        .setGroupId("dashboard-aggregator")    // 独立消费者组 2
        .build();
```

## 5. 踩坑

| 坑 | 现象 | 解法 |
|---|---|---|
| 事件类型不携带版本 | 新增字段后老消费者反序列化失败或语义误读 | 契约固化 schemaVersion 字段,消费方显式按版本分支 |
| 命令类事件被多消费者组重复消费 | 同一条告警被发送多次 | 命令类事件用单一消费者组(不广播),或引入去重层 |
| 分区键选错(如用时间戳分区) | 同一实体的事件散落多分区,顺序保证失效 | 分区键固定为业务实体 ID(VIN/用户 ID/订单 ID) |

## 6. 最佳实践

- 事件契约进独立仓库或独立目录版本化管理,变更走 PR 评审(类比 API 契约的 OpenAPI 治理)。
- 新增 Agent 订阅现有事实类事件时,不需要通知上游生产者——这是"广播语义"应该带来的解耦收益,若做不到说明设计有问题。

## 7. 面试题

① 命令类与事实类事件在消费语义上的本质差异?② Schema Registry 的"向后兼容"具体指什么操作允许、什么操作禁止?③ 分区键选择错误会在什么场景下才暴露出问题(提示:与 02 章 watermark/顺序性的关系)?

## 8. 参考资料

docs/01-06(序列化与 schema 演进);docs/07-01(Kafka 语义矩阵);Confluent Schema Registry 兼容性文档(向后/向前/完全兼容三种模式的通用行业实践)。
