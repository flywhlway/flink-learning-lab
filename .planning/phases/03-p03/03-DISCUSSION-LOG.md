# Phase 3: p03 大盘与演练收官 - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-18
**Phase:** 3-p03 大盘与演练收官
**Areas discussed:** Grafana 大盘数据源与面板, 异常检测规则落点, 压测范围与 baseline, 故障演练剧本, 文档包形态
**Mode:** `--auto --chain`（全部灰区自动选推荐项）

---

## Grafana 大盘数据源与面板

| Option | Description | Selected |
|--------|-------------|----------|
| 双数据源：CH 窗口指标 + Prometheus Flink | 对齐 ARCHITECTURE；业务可查、平台可观测 | ✓ |
| 仅 Prometheus Flink 指标 | 缺业务窗口聚合面板，难满足 VEH-05「窗口聚合」 | |
| 仅 ClickHouse | 丢失已接通 Flink 健康指标复用 | |

**Q: 窗口聚合放哪？**
| Option | Description | Selected |
|--------|-------------|----------|
| 独立窗口聚合作业（同 vehicle.events） | 不污染 CEP 主图 | ✓ |
| 塞进 VehicleAlertJob | 耦合高，难测 | |

**User's choice:** [auto] 双数据源 + 独立窗口作业
**Notes:** dashboard JSON 必须落仓可导入；default make up 不被 p03 硬绑定

---

## 异常检测规则落点

| Option | Description | Selected |
|--------|-------------|----------|
| Grafana 阈值/告警面板 + 文档化规则 | 满足「异常检测相关面板」且 MVP | ✓ |
| 新建 ML 异常作业 | 超出 Phase 边界 | |
| 再扩 CEP 模式当异常检测 | 与 VEH-05 语义错位、膨胀模式库 | |

**User's choice:** [auto] Grafana 阈值面板
**Notes:** 阈值须可演示；禁伪 SLA

---

## 压测范围与 baseline

| Option | Description | Selected |
|--------|-------------|----------|
| p03 项目级 loadtest + docs/baseline.md | VEH-06 硬验收且不抢 P5 | ✓ |
| 直接做 benchmark/ 全矩阵 | 属 PROD-01 / Phase 6 | |
| 引入 k6 新镜像 | 未进 SSOT | |

**User's choice:** [auto] 扩展 gen_vehicle_events + 项目级 baseline
**Notes:** 数字仅 OrbStack 实测；方法论引用 benchmark/README

---

## 故障演练剧本

| Option | Description | Selected |
|--------|-------------|----------|
| 恰好 2 条：watermark 停滞 + 负载 baseline | ROADMAP 点名 watermark；MVP | ✓ |
| watermark + 杀 TM + 断 Kafka 全套 | 范围膨胀 | |
| 仅文档 checklist 无脚本 | 违反可执行验收 | |

**User's choice:** [auto] 2 条可执行演练；额外 chaos 附录可选
**Notes:** 对齐 withIdleness + docs/02-time-window；失败非 0

---

## 文档包形态

| Option | Description | Selected |
|--------|-------------|----------|
| 1 ADR（Broadcast 预编译路线）+ RESUME.md + 架构短文 + 15-03 回填 | 覆盖 VEH-07 | ✓ |
| 多篇 ADR / 长篇白皮书 | 过度 | |
| 只改 README 无独立 ADR/简历页 | 不满足「可按路径打开」 | |

**User's choice:** [auto] ADR + RESUME + 架构补全 + docs/README 回填
**Notes:** 简历数字只引用 verify/baseline

---

## Claude's Discretion

- 窗口作业实现形态（DataStream vs Table/SQL）
- CH 指标表列设计、Grafana 面板布局
- 业务面板主查 CH 或经导出到 Prom 的最终接线
- 压测默认 eps/时长（以本机可绿为准）

## Deferred Ideas

- P5 全矩阵 benchmark、仓库级三块 Grafana、Loki/OTel、k6、杀 TM chaos
