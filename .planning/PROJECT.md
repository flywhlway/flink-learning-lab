# flink-learning-lab · P4–P6 里程碑

## What This Is

企业级 Apache Flink 全栈学习工程（2026 · AI 时代版）：教材 + Demo + 生产级项目 + 最佳实践合集。本里程碑在已交付的 P0–P3 基座上，落地三大生产级项目（车联网监控优先）、生产化能力与总装 QA，使仓库达到「可简历陈述、可一键复现、可压测演练」的完成态。

## Core Value

每个生产级项目必须在 OrbStack arm64 上独立 compose profile 一键起、端到端可复现，且压测与故障演练真实跑通——不可验证的内容不合入。

## Requirements

### Validated

- ✓ P0 基线：目录骨架、docker 一键环境、e01×3、SSOT 索引、路线/速查/题库首批 — v0.1.0
- ✓ P1 内核：docs 01–04 全文、e02–e04 + common、qa_check.sh — v0.2.0
- ✓ P2 SQL 与集成：docs 05–10、e05–e11、playground/templates — v0.3.0
- ✓ P3 AI 专书：ai/ 24 章 + e12 系列、Milvus ai-profile、docs 11 — v0.4.0
- ✓ 工程约定生效：版本 SSOT、八段式章节、docs/README.md 编号登记、约定式提交与 CHANGELOG — existing
- ✓ P4-p03 告警链路样板（VEH-01/02）：独立 compose profile + CEP→CH/Kafka 断言 — Phase 1
- ✓ P4-p03 模式库与 Broadcast（VEH-03/04）：≥3 五元组 + Kafka 控制面动态选择预编译模式 — Phase 2
- ✓ P4-p03 监控大盘与演练收官（VEH-05/06/07）：Grafana 双 DS + 压测/watermark 演练 + ADR/简历页 — Phase 3（浏览器 UAT 见 03-HUMAN-UAT.md）
- ✓ P4-p01 日志 AI 平台（LOG-01–05）：独立 compose profile + 可降级 AI 路径 + 压测/演练/ADR/简历页 — Phase 4
- ✓ P4-p02 实时推荐（RECO-01–03）：双通道特征 + 规则 Top-K + Redis 降级演练 + baseline/ADR/简历页 — Phase 5
- ✓ P4 三项目均满足：独立 compose profile 一键起、架构文档+ADR+验证脚本、压测与故障演练本机跑通 — Phase 1–5
- ✓ P5 生产化（PROD-01–04）：裁剪压测矩阵 + `benchmark/baseline.md`、Operator 1.15 Blue/Green 时间线、Argo CD + GitHub Actions、interview≥150 + 三块 Grafana JSON + best-practice 体系 — Phase 6
- ✓ P6 总装 QA（QA-01/02 + ENG-01…04）：`qa_check` 五硬门全绿；mains≥100；文档质量实质优先（已撤销 md≥30000 硬门，见 MEMORY D-14）；`eng_audit` ENG-01…04；README/PHASES/CHANGELOG 终稿；未打 git tag（D-12）— Phase 7

### Active

（无 — P4–P6 里程碑交付物已全部进入 Validated；git tag / GitHub Release 见 `/gsd-complete-milestone`）

### Out of Scope

- 升级主线到 Flink 2.3.0 — 连接器/Agents/Operator 生态未齐，见 ADR-001；2.3 仅在 docs/00-landscape 追踪
- 重写 P0–P3 已交付教材/Demo — 本里程碑只增量与交叉引用回填
- 沙箱内无法验证的「假装跑通」合入 — 违反不变量；半成品只进 wip/ 分支
- StateFun 新选型 — 社区已停运（docs/11 已确认）

## Context

- **接力协议入口**：阅读 PHASES.md 与 docs/README.md，继续 Phase N，遵守根 README 第 5 节。
- **已有基座可复用**：e10 CEP 车联网告警雏形、e07/e08 连接器与 CDC、e12 AI 路径、docker compose（Kafka/Flink/CH/PG/Redis/MinIO/Prom/Grafana）、Milvus ai-profile。
- **目标目录**：`projects/p03-vehicle-monitoring`、`p01-log-ai-platform`、`p02-realtime-reco` 均已达 P4 单项目完成态。
- **当前焦点**：P6 总装 QA 已达可验证完成态；下一步 `/gsd-complete-milestone`（打 tag，非本 Phase）。
- **会话粒度**：一个会话 ≤ 一个模块；先教材/架构章节，再 Demo/工程，再回填交叉引用。
- **中断恢复**：主干始终可 `make up`；半成品放 `wip/`。
- **受众**：已有 Flink 生产经验的架构师 → Enterprise Streaming Architect；三大项目同时服务简历陈述。

## Constraints

- **版本 SSOT**：根 README 版本矩阵 + `examples/pom.xml` 属性区；新增组件先登记再使用
- **文档编号**：先在 `docs/README.md` 登记；八段式结构强制（背景→架构→代码→启动→验证→踩坑→最佳实践→面试题+参考）
- **运行环境**：一切代码/命令必须在 OrbStack arm64 实测通过；不可验证不合入
- **内容禁令**：禁止 TODO、省略、略、自行实现、请参考官网
- **会话收尾**：更新 CHANGELOG 未发布区 + PHASES.md 状态列；约定式提交（如 `feat(p4): ...`）
- **验收**：每个 Phase 结束跑 `scripts/qa_check.sh`（编译、compose config、断链、违禁词、案例计数）
- **Tech stack**：Flink 2.2.1、JDK 21、Kafka 3.9.1、相关生态见版本矩阵

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| 本里程碑覆盖 P4+P5+P6 | 与 PHASES.md 总览对齐，一次规划避免碎片化 | — Pending |
| P4 交付顺序 p03→p01→p02 | e10 已预演车联网 CEP，最快出端到端硬验收样板 | — Pending |
| P4 验收三项全硬 | compose / ADR+验证脚本 / 压测故障演练均不可降级 | — Pending |
| GSD 按交付物切细（约 5–7 phase） | 会话粒度 ≤ 一模块；避免单 phase 过大 | — Pending |
| p03：先告警链路，后监控大盘 | 两者都要；告警先证明 CEP 端到端可复现 | ✓ Phase 1–3 全套已交付（大盘/演练/ADR） |
| 跳过 codebase map | 用户熟悉仓库；以 PHASES/docs/已交付产物为上下文 | ✓ Good |
| 主线锁定 Flink 2.2.1 | ADR-001：生态兼容优先于最新号 | ✓ Good |
| P5 压测默认 compose Flink | OrbStack 稳定；K8s 留给 Operator BG 硬门禁 | ✓ Phase 6 |
| GitOps 单一路径锁定 Argo CD | STACK 拒绝双栈；本机用 git-daemon 镜像演示 sync | ✓ Phase 6 |
| 反压值班指标名 | 本机 Prom 为 `backPressuredTimeMsPerSecond`（非 is*） | ✓ Phase 6 review 修复 |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-07-18 after Phase 6 complete*
