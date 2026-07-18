# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v1.0 — P4–P6 Production

**Shipped:** 2026-07-19  
**Phases:** 7 | **Plans:** 32 | **Tasks:** 82

### What Was Built
- p03 车联网：独立 compose profile → CEP 告警 → ≥3 模式 Broadcast → Grafana 双 DS + watermark 演练 + ADR/RESUME
- p01 日志 AI：默认可降级规则路径 + Async Ollama 旁路 + 护栏/预算指标 + 全套工程纪律
- p02 实时推荐：双通道特征 + 规则 Top-K + Redis 降级演练 + baseline
- P5：裁剪压测矩阵、Operator 1.15 Blue/Green、Argo CD GitOps、interview≥150、三块 Grafana JSON
- P6：`qa_check` + `eng_audit` 双绿；mains≥100；撤销文档行数硬门并回退注水

### What Worked
- Vertical MVP：先打 p03 样板再复制纪律到 p01/p02，跨项目一致性高
- Nyquist Wave 0 RED 夹具：失败态脚本/单测先行，避免假绿
- 独立 compose profile：default `make up` 始终可用
- 里程碑审计 + 文档注水事故当日回退：MEMORY D-14…D-16 与门禁拆除闭环干净

### What Was Inefficient
- Phase 7 Wave 2 以行数硬门驱动导致大规模注水，随后整段回退（成本高）
- Phase 3 人工 UAT（浏览器/UI/简历可读性）未在关闭前补签，留下 tech_debt
- 若干 VALIDATION.md 任务表停留在 W0 pending 表象，与已通过 VERIFICATION 不同步

### Patterns Established
- 单项目完成态五件套：profile + verify 断言 + loadtest/drill + ARCHITECTURE/ADR/RESUME + docs 登记
- CH count 为告警权威放行；Kafka 仅诊断
- AI 路径默认 off；显式 DEGRADE-CHECKLIST
- P6 起 ENG 审计与 qa_check 并列硬门；文档质量实质优先、不以行数硬门禁

### Key Lessons
1. 计量指标必须可防注水；行数类硬门会扭曲交付行为
2. `human_needed` 应在里程碑关闭前显式 Acknowledge 或补签，避免审计噪音
3. 跨 Phase 数据契约（如 `pattern_id`）应在 VERIFICATION 中写明消费方，集成检查才可自动映射

### Cost Observations
- Timeline: 2026-07-17 → 2026-07-19（约 2 天主执行 + 次日 QA 勘误）
- Notable: Phase 7 文档轴返工是本里程碑最大非功能成本

### Known Deferred at Close
- Phase 03 HUMAN-UAT 3 scenarios pending；03-VERIFICATION human_needed
- Phase 02 VALIDATION nyquist_compliant=false 元数据未回填（VERIFICATION 已 passed）

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.0 | multi | 7 | 首次 GSD 全里程碑：Vertical MVP + Nyquist + 总装双绿；发现行数硬门反模式 |

### Cumulative Quality

| Milestone | Hard gates | Cases (mains) | Notable |
|-----------|------------|---------------|---------|
| v1.0 | qa_check + eng_audit | 100 | 撤销 md 行数硬门；OrbStack 实测纪律 |

### Top Lessons (Verified Across Milestones)

1. 不可验证不合入（Core Value）在三项目上可复制
2. 硬门禁指标须抗博弈；实质质量 > 表面计量
