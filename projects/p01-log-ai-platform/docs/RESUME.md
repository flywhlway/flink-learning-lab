# p01 · 简历陈述页（可复现）

> 每条陈述对应仓库内命令或路径。数字只引用 [baseline.md](baseline.md) / `verify` 实测；禁止空泛形容词。Lab 账号见项目 README（ClickHouse `flinklab` / `flinklab123`）。

## 一句话

在 OrbStack arm64 上交付可降级的结构化日志 AI 平台：独立 compose profile `p01`、默认规则路径零外部模型、可选 Async Ollama 风险分级旁路，并用脚本断言 ClickHouse 权威出口、AI 关闭仍绿，以及项目级压测 baseline。

## 可复现陈述（动词 → 路径）

| 陈述 | 复现命令 / 路径 |
|------|-----------------|
| 一键起 p01 topic 与 CH DDL，不影响 default `make up` | `cd docker && make up && make init && make up-p01` |
| 打包并提交默认 AI off 作业 | `cd projects/p01-log-ai-platform && make package && make submit` |
| 用 CH `rule_label=AUTH_FAIL` 断言规则路径 | `make gen` → `make verify`（`scripts/verify.sh`） |
| 一键清空造数并轮询 CH | `make match` |
| 演示 AI 关闭 / endpoint 不可达时门禁仍绿 | `make drill-degrade`（`scripts/drill_ai_degrade.sh`） |
| 有 Ollama 时跑可选 AI 验收轨 | `make verify-ai AI_MODEL=<本机模型>`（`scripts/verify_ai.sh`） |
| 跑项目级压测并留下 baseline 表 | `make loadtest` → [docs/baseline.md](baseline.md) |
| 说明为何主构建零硬依赖 Preview / 外部模型 | [docs/adr/0001-ai-path-degradable.md](adr/0001-ai-path-degradable.md) |
| 说明 Parse→…→BudgetGate→Async→Guardrail→CH 顺序 | [docs/ARCHITECTURE.md](ARCHITECTURE.md) |
| 勾选无 Ollama / 无 Milvus / AI 关闭 / 超时降级 | [docs/DEGRADE-CHECKLIST.md](DEGRADE-CHECKLIST.md) |

## 验收纪律（面试可答）

1. **权威出口是 ClickHouse**，不是 Kafka：`verify.sh` 只认 `flinklab.log_results` 的 `rule_label`；Kafka 仅诊断（D-10）。
2. **默认 AI off**：`JobConfig` 默认 + `make submit --ai.enabled=false`；无 Ollama 时作业仍 RUNNING，`ai_source=DISABLED`。
3. **BudgetGate 在 Async 前、Guardrail 在 Sink 前**：超限短路外呼；输出侧 BLOCK 仍落库但脱敏（见 ARCHITECTURE）。
4. **恰好两条硬演练**：`make loadtest` + `make drill-degrade`（D-14）；额外 chaos 不挡完成。

## 实测数字摘录（仅 baseline）

来源：[docs/baseline.md](baseline.md)（`make loadtest` 于 2026-07-18 OrbStack arm64 写入；**非生产 SLA**）。

| 项 | 值 |
|----|-----|
| 负载 | 100 eps × 90s（热身 30s 丢弃） |
| 墙钟折算 produce rate | 100.0 eps |
| `lastCheckpointDuration`（p01 jobs，max） | 61.0 ms |
| `numRestarts`（p01 jobs，max） | 0.0 |
| `currentEmitEventTimeLag`（kafka_log_events source，max） | 8.0 ms |
| CH `log_results` 增量（meter 前后） | 9305 |

复跑：`cd projects/p01-log-ai-platform && make loadtest`。

## 技术栈锚点（SSOT）

Flink 2.2.1 · JDK 21 · Kafka connector 5.0.0-2.2 · ClickHouse 24.8 · 宿主机 Ollama（可选）· Prometheus / Grafana（版本见仓库根 README 矩阵）。架构与交叉引用见 [ARCHITECTURE.md](ARCHITECTURE.md)。
