# Roadmap: flink-learning-lab P4–P6

## Overview

在 P0–P3 基座上，用 Vertical MVP 切片交付：先以 p03 车联网打出「compose profile + 端到端断言 + 压测/演练 + ADR」样板（告警→模式库→大盘），再复制纪律完成 p01 日志 AI 与 p02 实时推荐，随后落地 P5 生产化（Operator/GitOps/压测矩阵），最后以 P6 总装 QA 收官。对应仓库 PHASES.md 的 P4→P5→P6。

## Phases

- [x] **Phase 1: p03 告警链路样板** - compose profile + 事件→CEP→告警落库可复现 (completed 2026-07-17)
- [x] **Phase 2: p03 模式库与 Broadcast** - ≥3 模式五元组 + 动态选择预编译模式 (completed 2026-07-18)
- [ ] **Phase 3: p03 大盘与演练收官** - Grafana 大盘 + 压测/故障演练 + 完整文档包
- [ ] **Phase 4: p01 日志 AI 平台** - 可降级 AI 路径 + 生产项目纪律全套
- [ ] **Phase 5: p02 实时推荐** - 特征+打分闭环 + 生产项目纪律全套
- [ ] **Phase 6: P5 生产化** - benchmark / Operator Blue-Green / GitOps / 规范题库看板
- [ ] **Phase 7: P6 总装 QA** - qa_check 全绿 + 计量达标 + 工程不变量终检

## Phase Details

### Phase 1: p03 告警链路样板

**Goal:** As a 仓库维护者, I want to 用独立 p03 compose profile 一键拉起并复现事件→Kafka→CEP→Side Output→ClickHouse/Kafka 告警链路且用断言脚本验收, so that default make up 不受影响且告警可观察、验证失败非 0.
**Mode:** mvp
**Depends on:** Nothing (first phase；依赖已交付 P0–P3 基座)
**Requirements:** VEH-01, VEH-02
**Success Criteria** (what must be TRUE):

  1. 维护者执行 p03 profile 启动后，default `make up` 仍可用
  2. 造数后可在 ClickHouse/Kafka 告警通道观察到 CEP 匹配或旁路输出
  3. 验证脚本失败时非 0 退出（含断言，而非仅 echo）

**Plans:** 4/4 plans complete
Plans:
**Wave 1**

- [x] 01-00-PLAN.md — Wave 0：Nyquist 夹具（surefire + 失败态单测/verify）
- [x] 01-01-PLAN.md — VEH-01：p03 compose profile 隔离与 topic/表初始化

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-PLAN.md — VEH-02：CEP 作业 + Parse/Sink + package/submit + 单测

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-03-PLAN.md — VEH-02：造数 + e2e verify + 八段式文档 + 人工验收

### Phase 2: p03 模式库与 Broadcast

**Goal:** As a 仓库维护者, I want to 维护恰好 3 条可评审 CEP 模式库并通过 Kafka Broadcast 控制消息切换预编译激活集、在 ClickHouse 按 pattern_id 断言匹配变化, so that 无 within 不得合入且不必重启作业即可演示开源 CEP 动态化路线.
**Mode:** mvp
**Depends on:** Phase 1
**Requirements:** VEH-03, VEH-04
**Success Criteria** (what must be TRUE):

  1. 文档中至少 3 条模式均含 within/连接语义/skip/状态上界五元组
  2. 维护者可通过 Broadcast 配置切换模式集并观察到匹配行为变化
  3. 无 within 的模式无法通过项目自检/评审清单

**Plans:** 5/5 plans complete
Plans:
**Wave 0**

- [x] 02-00-PLAN.md — Wave 0：Nyquist RED 夹具（Registry within / 新模式 / Gate / verify PATTERN_ID）

**Wave 1** *(blocked on Wave 0)*

- [x] 02-01-PLAN.md — VEH-03：三工厂 + PatternRegistry + PATTERN-LIBRARY 五元组

**Wave 2** *(blocked on Wave 1)*

- [x] 02-02-PLAN.md — VEH-04：PatternControlMessage + PatternActivationGate 单测 GREEN

**Wave 3** *(blocked on Wave 2)*

- [x] 02-02b-PLAN.md — VEH-04：三 CEP 作业接线 + pattern_id DDL/Sink + control topic

**Wave 4** *(blocked on Wave 3)*

- [x] 02-03-PLAN.md — VEH-03/04：造数切换 e2e + README/qa_check + 人工验收

### Phase 3: p03 大盘与演练收官

**Goal:** As a 仓库维护者, I want to 复现 Grafana 双数据源大盘、执行 watermark 停滞与压测 baseline、并打开架构/ADR/简历陈述页, so that p03 达到 PHASES P4 单项目完成态（可简历陈述、可一键复现、可压测演练）.
**Mode:** mvp
**Depends on:** Phase 2
**Requirements:** VEH-05, VEH-06, VEH-07
**Success Criteria** (what must be TRUE):

  1. Grafana 可展示窗口聚合指标与异常检测相关面板
  2. 压测与故障演练（含 watermark 停滞）可按剧本执行并留下 baseline 数字
  3. 架构文档、ADR、验证脚本与简历陈述页齐全且可按路径打开

**Plans:** 4 plans

Plans:
**Wave 0**

- [ ] 03-00-PLAN.md — Wave 0：Nyquist RED（verify_dashboard/loadtest/drill 骨架 + EventCountAggTest + DDL/Grafana 钩子）

**Wave 1** *(blocked on Wave 0)*

- [ ] 03-01-PLAN.md — VEH-05：窗口作业→CH→Grafana 双 DS 大盘 + 异常阈值 + verify_dashboard

**Wave 2** *(blocked on Wave 1)*

- [ ] 03-02-PLAN.md — VEH-06：gen --rate/--duration + loadtest→baseline + watermark 停滞演练

**Wave 3** *(blocked on Wave 2)*

- [ ] 03-03-PLAN.md — VEH-07：ARCHITECTURE + ADR + RESUME + 15-03/CHANGELOG/PHASES + qa_check

### Phase 4: p01 日志 AI 平台

**Goal:** 交付日志 AI 生产级项目：无 AI 可跑的特征路径 + 可降级 AI 路径 + 全套工程纪律
**Mode:** mvp
**Depends on:** Phase 3（复用 p03 工程样板）
**Requirements:** LOG-01, LOG-02, LOG-03, LOG-04, LOG-05
**Success Criteria** (what must be TRUE):

  1. p01 profile 一键起，结构化日志流端到端可复现
  2. 关闭 Ollama/Milvus/Agents 时，富化/特征路径仍可按降级清单演示
  3. 至少一条 AI 路径在启用环境下可观察输出；成本/护栏指标可见
  4. 压测、故障演练、架构/ADR/验证脚本/简历陈述齐全

**Plans:** TBD

### Phase 5: p02 实时推荐

**Goal:** 交付实时推荐生产级项目：行为流→特征→召回/打分闭环 + 全套工程纪律
**Mode:** mvp
**Depends on:** Phase 3（工程样板；可与 Phase 4 规划并行，执行建议串行以降低风险）
**Requirements:** RECO-01, RECO-02, RECO-03
**Success Criteria** (what must be TRUE):

  1. p02 profile 一键起，行为事件可进入作业
  2. 特征与打分结果可在 Kafka/存储中观察
  3. 压测、故障演练、架构/ADR/验证脚本/简历陈述齐全

**Plans:** TBD

### Phase 6: P5 生产化

**Goal:** 落地压测矩阵、Operator Blue/Green、GitOps、规范与题库/看板，且均在 OrbStack 可观察
**Mode:** mvp
**Depends on:** Phase 5（至少一至三个项目作业可作为 Operator/压测对象）
**Requirements:** PROD-01, PROD-02, PROD-03, PROD-04
**Success Criteria** (what must be TRUE):

  1. benchmark 矩阵可运行并生成 baseline.md
  2. OrbStack K8s 上完成 Operator 1.15 Blue/Green 演练，有可观察事件/日志时间线
  3. 按文档可复现单一 GitOps/CI-CD 路径
  4. best-practice 完整、interview ≥150、monitoring 看板 JSON 可导入

**Plans:** TBD

### Phase 7: P6 总装 QA

**Goal:** 全仓质量门禁与计量达标，工程不变量终检，里程碑可打 tag
**Mode:** mvp
**Depends on:** Phase 6
**Requirements:** QA-01, QA-02, ENG-01, ENG-02, ENG-03, ENG-04
**Success Criteria** (what must be TRUE):

  1. `scripts/qa_check.sh` 全绿
  2. 案例 ≥100、文档 ≥30k 行，README 与 PHASES 状态一致
  3. 版本 SSOT/编号登记/违禁词/约定式提交与 CHANGELOG 纪律通过终检（ENG-*）

**Plans:** TBD

**Note on ENG-*:** 工程不变量自 Phase 1 起每会话强制执行；Phase 7 做终检与追溯关闭。

## Progress

**Execution Order:**
1 → 2 → 3 → 4 → 5 → 6 → 7

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. p03 告警链路样板 | 4/4 | Complete    | 2026-07-17 |
| 2. p03 模式库与 Broadcast | 5/5 | Complete    | 2026-07-18 |
| 3. p03 大盘与演练收官 | 0/4 | Not started | - |
| 4. p01 日志 AI 平台 | 0/TBD | Not started | - |
| 5. p02 实时推荐 | 0/TBD | Not started | - |
| 6. P5 生产化 | 0/TBD | Not started | - |
| 7. P6 总装 QA | 0/TBD | Not started | - |

## Coverage Validation

| Requirement | Phase |
|-------------|-------|
| VEH-01 | 1 |
| VEH-02 | 1 |
| VEH-03 | 2 |
| VEH-04 | 2 |
| VEH-05 | 3 |
| VEH-06 | 3 |
| VEH-07 | 3 |
| LOG-01 | 4 |
| LOG-02 | 4 |
| LOG-03 | 4 |
| LOG-04 | 4 |
| LOG-05 | 4 |
| RECO-01 | 5 |
| RECO-02 | 5 |
| RECO-03 | 5 |
| PROD-01 | 6 |
| PROD-02 | 6 |
| PROD-03 | 6 |
| PROD-04 | 6 |
| QA-01 | 7 |
| QA-02 | 7 |
| ENG-01 | 7 |
| ENG-02 | 7 |
| ENG-03 | 7 |
| ENG-04 | 7 |

**Coverage:** 25/25 v1 requirements mapped ✓

---
*Roadmap created: 2026-07-17*
