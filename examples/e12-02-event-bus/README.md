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

---

# e12-02-event-bus · 八段式扩写（Wave 2）

## 1. 背景

本模块演示「流式事件总线」。目标是在零依赖或受控依赖下跑通机制，而不是堆模型。对应教材章节：`../../ai/chapters/`（ai/02）。生产降级对照 p01。

## 2. 架构

```mermaid
flowchart LR
  In[事件源 datagen/Kafka] --> Op[本模块算子]
  Op --> Out[主流输出]
  Op --> Side[旁路/降级]
```

算子链保持可观测：主流契约稳定，超时/拒识/超预算走旁路。主类焦点：Kafka 主题契约与多消费者。

## 3. 代码锚点

阅读 `src/main/java/**/*.java` 中带 `public static void main` 的作业；注意 `.uid(...)` 与旁路 OutputTag。模块坐标：`examples/e12-02-event-bus`。

## 4. 启动

```bash
(cd docker && docker compose up -d)  # 若需要基座
(cd examples && mvn -pl e12-02-event-bus -am -DskipTests package)
# 提交主类见下方表格；OrbStack arm64 实测
```

## 5. 验证

- UI RUNNING
- 主流有输出；注入故障后旁路有信号
- `mvn -pl e12-02-event-bus -am -DskipTests compile` 通过
- 不引入违禁词

## 6. 踩坑

| 症状 | 根因 | 处置 |
|---|---|---|
| 作业起不来 | 类路径/主类 | 核对 pom 与 -c |
| 无输出 | 源无数据/过滤过严 | 查 datagen 与旁路 |
| 外呼拖死 | 同步阻塞 | 改 Async / 降级 |
| 成本飙升 | 无预算门控 | 软顶+降采样 |

## 7. 最佳实践

- 有状态算子固定 uid；见 `../../best-practice/02-uid-savepoint.md`
- AI/外呼路径必须可降级；见 `../../best-practice/08-ai-degrade.md`
- 反压按三步法；见 `../../best-practice/05-backpressure.md`
- 交叉教材：`../../docs/` 与 `../../ai/chapters/`

## 8. 面试题

对应 `../../interview/L8.md`（AI）或模块相关 Level；用 90 秒讲清定义→机制→反例→仓库路径。


## 深潜 1

围绕「流式事件总线」第 1 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 2

围绕「流式事件总线」第 2 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 3

围绕「流式事件总线」第 3 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 4

围绕「流式事件总线」第 4 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 5

围绕「流式事件总线」第 5 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 与生产项目对照

- p01：`../../projects/p01-log-ai-platform/README.md`（AI off 默认可跑）
- p02：特征/召回对照（若主题相关）
- 规范：`../../best-practice/08-ai-degrade.md`

## 验证记录模板

日期 / 环境 OrbStack / 命令 / 期望 / 实际 / 日志路径。通过后才可在笔记中勾选本模块。

