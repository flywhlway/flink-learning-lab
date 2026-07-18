# e12-15 · Streaming Observability:Metrics + 结构化日志

> 对应 [ai/chapters/15-streaming-observability.md](../../ai/chapters/15-streaming-observability.md) · Level:L4
> 运行:`mvn -q -Plocal compile exec:java -pl e12-15-observability -Dexec.mainClass=com.flywhl.flinklab.e12.ObservableAgentJob`

## 背景

不依赖 Flink Agents 框架自带的 EventLog,任何 DataStream 算子都能用本 Demo 的模式接入完整可观测性:自定义 Counter/Histogram + 结构化日志。本仓库从 Phase 0 就搭好的 Prometheus/Grafana 管道在此直接复用。

## 验证方式

本地运行观察控制台的结构化日志行(`agent_decision event_user=... latency_ms=...`);提交到集群后可在 Grafana/WebUI 看到 `alerts_triggered` 与 `decision_latency_ms` 指标。

## 源码要点

- `DropwizardHistogramWrapper` 是 Flink 官方提供的桥接类,把 Dropwizard Metrics 的 Histogram 接入 Flink Metrics 体系。
- 结构化日志字段命名建议全仓库统一(event_user/action/latency_ms 等),便于跨作业聚合分析。

## 面试题

见 ai/chapters/15-streaming-observability.md 第 8 节。

---

# e12-15-observability · 八段式扩写（Wave 2）

## 1. 背景

本模块演示「可观测」。目标是在零依赖或受控依赖下跑通机制，而不是堆模型。对应教材章节：`../../ai/chapters/`（ai/15）。生产降级对照 p01。

## 2. 架构

```mermaid
flowchart LR
  In[事件源 datagen/Kafka] --> Op[本模块算子]
  Op --> Out[主流输出]
  Op --> Side[旁路/降级]
```

算子链保持可观测：主流契约稳定，超时/拒识/超预算走旁路。主类焦点：SlowOpMark 与追踪字段。

## 3. 代码锚点

阅读 `src/main/java/**/*.java` 中带 `public static void main` 的作业；注意 `.uid(...)` 与旁路 OutputTag。模块坐标：`examples/e12-15-observability`。

## 4. 启动

```bash
(cd docker && docker compose up -d)  # 若需要基座
(cd examples && mvn -pl e12-15-observability -am -DskipTests package)
# 提交主类见下方表格；OrbStack arm64 实测
```

## 5. 验证

- UI RUNNING
- 主流有输出；注入故障后旁路有信号
- `mvn -pl e12-15-observability -am -DskipTests compile` 通过
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

围绕「可观测」第 1 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 2

围绕「可观测」第 2 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 3

围绕「可观测」第 3 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 4

围绕「可观测」第 4 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 5

围绕「可观测」第 5 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 与生产项目对照

- p01：`../../projects/p01-log-ai-platform/README.md`（AI off 默认可跑）
- p02：特征/召回对照（若主题相关）
- 规范：`../../best-practice/08-ai-degrade.md`

## 验证记录模板

日期 / 环境 OrbStack / 命令 / 期望 / 实际 / 日志路径。通过后才可在笔记中勾选本模块。

