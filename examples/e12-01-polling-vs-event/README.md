# e12-01 · 轮询 vs 事件驱动:延迟对照实验

> 对应 [ai/chapters/01-why-streaming.md](../../ai/chapters/01-why-streaming.md) · Level:L1
> 运行:`mvn -q -Plocal compile exec:java -pl e12-01-polling-vs-event -Dexec.mainClass=com.flywhl.flinklab.e12.PollingVsEventDrivenJob`

## 背景

全书第一个论点——"Streaming 是 Agent 进入生产的前提条件"——不该只是断言,本 Demo 用同一批异常事件跑两种检测方式,把延迟差异变成可测量的数字。

## 验证方式

运行后观察两种前缀的输出:`EVENT-DRIVEN` 行的延迟恒为个位数到两位数毫秒;`POLLING` 行的延迟分布在 `[0, 2000]` 毫秒之间(轮询间隔=2秒)。多跑几次可以直观看到轮询延迟的随机性(取决于异常恰好落在轮询周期的哪个位置),而事件驱动延迟始终稳定。

## 源码要点

- 两个分支共享同一份输入事件(`Labs.events`),保证对比公平。
- 轮询分支用**处理时间定时器**模拟"每 N 秒扫一次"的批量检查行为——用 Flink 的定时器机制模拟一个本不需要 Flink 的传统轮询系统,恰好说明了"能力过剩"与"能力不足"的两端都不是好的架构选择。
- 把 `POLL_INTERVAL_MS` 改小可以观察延迟下降,但代价是定时器触发频率上升(对应真实系统里"扫描频率上升、系统负载上升"),这正是 ai/01 第 5 节论证的两难。

## 面试题

见 ai/chapters/01-why-streaming.md 第 7 节。

---

# e12-01-polling-vs-event · 八段式扩写（Wave 2）

## 1. 背景

本模块演示「轮询 vs 事件驱动基线」。目标是在零依赖或受控依赖下跑通机制，而不是堆模型。对应教材章节：`../../ai/chapters/`（docs/00 + ai/01）。生产降级对照 p01。

## 2. 架构

```mermaid
flowchart LR
  In[事件源 datagen/Kafka] --> Op[本模块算子]
  Op --> Out[主流输出]
  Op --> Side[旁路/降级]
```

算子链保持可观测：主流契约稳定，超时/拒识/超预算走旁路。主类焦点：EventDrivenOnlyBaselineJob / 对照轮询心智。

## 3. 代码锚点

阅读 `src/main/java/**/*.java` 中带 `public static void main` 的作业；注意 `.uid(...)` 与旁路 OutputTag。模块坐标：`examples/e12-01-polling-vs-event`。

## 4. 启动

```bash
(cd docker && docker compose up -d)  # 若需要基座
(cd examples && mvn -pl e12-01-polling-vs-event -am -DskipTests package)
# 提交主类见下方表格；OrbStack arm64 实测
```

## 5. 验证

- UI RUNNING
- 主流有输出；注入故障后旁路有信号
- `mvn -pl e12-01-polling-vs-event -am -DskipTests compile` 通过
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

围绕「轮询 vs 事件驱动基线」第 1 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 2

围绕「轮询 vs 事件驱动基线」第 2 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 3

围绕「轮询 vs 事件驱动基线」第 3 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 4

围绕「轮询 vs 事件驱动基线」第 4 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 5

围绕「轮询 vs 事件驱动基线」第 5 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 与生产项目对照

- p01：`../../projects/p01-log-ai-platform/README.md`（AI off 默认可跑）
- p02：特征/召回对照（若主题相关）
- 规范：`../../best-practice/08-ai-degrade.md`

## 验证记录模板

日期 / 环境 OrbStack / 命令 / 期望 / 实际 / 日志路径。通过后才可在笔记中勾选本模块。

