# Project Research Summary

**Project:** flink-learning-lab · P4–P6 里程碑
**Domain:** Flink 企业级流处理学习工程（生产 showcase + 生产化）
**Researched:** 2026-07-17
**Confidence:** HIGH
**Note:** 研究员子代理因 API 限额不可用；由编排器基于 SSOT/PHASES/e10 内联完成后续里程碑调研。

## Executive Summary

本里程碑不是从零选栈，而是在 **Flink 2.2.1 + 已交付 docker/examples/ai** 之上，交付三个可独立复现的生产级项目，再完成 Operator/GitOps/压测与总装 QA。核心风险不是「不会写 Flink」，而是 **假完成**（文档齐全但 OrbStack 未跑）、**破坏基座 compose**、以及 **CEP/AI 路径缺少上界与降级**。

推荐做法：用 **p03 车联网** 打出「compose profile + 验证断言 + 压测/演练 + ADR」样板，再复制纪律到 p01/p02；P5 只在有真实作业后做 Blue/Green；P6 做计量与违禁词清零。

## Key Findings

### Recommended Stack

沿用 README 版本矩阵；禁止升 Flink 2.3。p03 以 CEP+Kafka+CH+Grafana 为主；p01 叠加 Milvus/Ollama/Agents（可降级）；p02 以特征+Redis+打分为主；P5 用 Operator 1.15 + 单一 GitOps。

**Core technologies:**
- Flink 2.2.1 / JDK 21 — 锁定主线
- Kafka 3.9.1 + CEP — p03 事件与模式
- ClickHouse + Prometheus/Grafana — 落库与大盘
- Operator 1.15 — P5 Blue/Green

### Expected Features

**Must have (table stakes):**
- 独立 compose profile 一键起
- 端到端可复现 + 验证脚本断言
- 架构文档 + ADR + 简历陈述
- 压测 + 故障演练可执行
- p03：告警链路先、大盘后；模式库≥3 + 五元组
- p01：至少一条 AI 路径 + 降级
- P5：benchmark/Operator/GitOps/题库150+/看板 JSON
- P6：qa_check 全绿、案例≥100、文档≥30k

**Should have (competitive):**
- Broadcast 动态选择预编译模式
- 统一 drills/bench 目录结构
- p01 成本/护栏可观测

**Defer (v2+):**
- 商业动态 CEP、完整多租户 SaaS、双 GitOps 栈

### Architecture Approach

共享 `docker/` + per-project `projects/pNN-*` + compose profiles。教学 `examples/` 不膨胀成生产项目。构建顺序：p03 告警 → 模式库 → 大盘 → p01 → p02 → P5 → P6。

### Critical Pitfalls

1. 假完成 / 沙箱豁免当 ✅  
2. 破坏 `make up`  
3. CEP 无 within 状态爆炸  
4. AI 无降级  
5. 压测演练写成散文  
6. K8s Blue/Green 纸面  

## Roadmap Implications

### Suggested Phase Structure

| Phase | Name | Rationale |
|-------|------|-----------|
| 1 | p03 告警链路样板 | 最快复用 e10；建立验收样板 |
| 2 | p03 模式库 + Broadcast | 生产 CEP 深度 |
| 3 | p03 监控大盘 + 压测演练 | 用户要求的第二会话能力 |
| 4 | p01 日志 AI 平台 | 复制纪律 + AI 降级 |
| 5 | p02 实时推荐 | 三项目收齐 |
| 6 | P5 生产化 | Operator/GitOps/benchmark/规范/题库/看板 |
| 7 | P6 总装 QA | 计量与门禁全绿 |

（Standard 粒度约 5–8；上表 7 相契合。若需压缩：Phase2+3 可合并，但不建议合并跨项目。）

### Phase Ordering Constraints

- p03 样板 → p01/p02（工程纪律依赖）
- P4 作业可跑 → P5 Operator（避免空转）
- P5/P4 稳定 → P6 计量

### Requirements Flags

- [ ] 验证脚本必须含断言（非 echo）— 防假完成
- [ ] p01 降级路径为 REQ 非子注 — 防环境耦合
- [ ] P5 Blue/Green 可观察成功标准 — 防纸面
- [ ] 每 Phase 结束 qa_check — 不变量

## Open Questions

无阻塞项。版本与顺序已由用户与 ADR-001 锁定。执行期仅需本机确认 Agents/Operator 细节。

## Sources

- `.planning/PROJECT.md`
- `README.md` 版本矩阵 / ADR-001
- `PHASES.md`
- `examples/e10-cep/README.md`
- `CHANGELOG.md` P2/P3 Notes
- `.planning/research/{STACK,FEATURES,ARCHITECTURE,PITFALLS}.md`

---
*Research completed: 2026-07-17*
