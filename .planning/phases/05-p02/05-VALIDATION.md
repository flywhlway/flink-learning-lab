---
phase: 5
slug: p02
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-18
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `05-RESEARCH.md` § Validation Architecture.
> Task IDs aligned to PLAN.md files by planner (2026-07-18).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit Jupiter 5.10.2 + Maven Surefire 3.2.5 |
| **Config file** | `projects/p02-realtime-reco/pom.xml`（surefire；Wave 0 新建） |
| **Quick run command** | `cd projects/p02-realtime-reco && mvn -q -Dtest=RuleScorerTest,ParseBehaviorJsonTest test` |
| **Full suite command** | `cd projects/p02-realtime-reco && mvn -q test` |
| **Estimated runtime** | ~30–90 seconds（单测）；E2E 另计 |

---

## Sampling Rate

- **After every task commit:** Run quick run command（或相关单测子集）
- **After every plan wave:** `mvn -q test` + `bash -n scripts/*.sh`（若环境齐再 `make match`）
- **Before `/gsd-verify-work`:** Full suite green + `make match` + Redis 降级演练 + baseline
- **Max feedback latency:** 120 seconds（单测）；E2E 按文档

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 05-00-01 | 05-00 | 0 | RECO-01/02 | T-05-01/T-05-05 | jedis SSOT；Parse 拒脏字段（RED） | unit | `mvn -q -Dtest=ParseBehaviorJsonTest,RuleScorerTest test` 期望 ≠0 | ❌ W0 | ⬜ pending |
| 05-00-02 | 05-00 | 0 | RECO-01/03 | T-05-01 | verify 骨架 CH 权威；脚本默认非 0 | script | `bash -n scripts/{verify,drill_redis_degrade,loadtest}.sh` + 直接执行 ≠0 | ❌ W0 | ⬜ pending |
| 05-00-03 | 05-00 | 0 | RECO-01 | T-05-06 | p02-init 不污染 default up | smoke | `docker compose --profile p02 config -q`；grep up-p02 | ❌ W0 | ⬜ pending |
| 05-01-01 | 05-01 | 1 | RECO-01 | T-05-01 | 行为 JSON 校验；脏数据丢弃 | unit | `mvn -q -Dtest=ParseBehaviorJsonTest test` | ❌ W0 | ⬜ pending |
| 05-01-02 | 05-01 | 1 | RECO-01 | T-05-04 | JobConfig/topK 上限；造数 MAX_RATE | package | `mvn -q -DskipTests package`；`py_compile gen_reco_events.py` | ❌ W0 | ⬜ pending |
| 05-01-03 | 05-01 | 1 | RECO-01 | — | profile 隔离冒烟 | smoke | `bash scripts/smoke_p02_profile.sh` | ❌ W0 | ⬜ pending |
| 05-02-01 | 05-02 | 2 | RECO-02 | T-05-02/T-05-03 | Redis key 仅校验 userId 拼接；写失败不打挂 | unit | `mvn -q -Dtest=SessionFeatureFunctionTest test` | ❌ W0 | ⬜ pending |
| 05-02-02 | 05-02 | 2 | RECO-02 | T-05-03 | 规则 Top-K；STATE_ONLY/REDIS 分支 | unit | `mvn -q -Dtest=RuleScorerTest test` | ❌ W0 | ⬜ pending |
| 05-02-03 | 05-02 | 2 | RECO-02 | T-05-01 | CH Sink 拒注入；verify SQL 白名单；CH 放行 | e2e | `make match` → `scripts/verify.sh` | ❌ W0 | ⬜ pending |
| 05-03-01 | 05-03 | 3 | RECO-03 | T-05-01/T-05-03 | Redis 降级仍出 CH STATE_ONLY 行 | e2e drill | `make drill-redis` | ❌ W0 | ⬜ pending |
| 05-03-02 | 05-03 | 3 | RECO-03 | T-05-03 | loadtest → baseline 非空实测段 | script/doc | `make loadtest`；`docs/baseline.md` | ❌ W0 | ⬜ pending |
| 05-03-03 | 05-03 | 3 | RECO-03 | T-05-06 | ADR/ARCHITECTURE/RESUME + 15-02 + qa_check | doc | `test -f docs/adr/0001-*.md` 等；`bash scripts/qa_check.sh` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Task IDs = `{phase}-{plan}-{task}` matching PLAN.md task order.*

---

## Wave 0 Requirements

- [ ] `projects/p02-realtime-reco/` 工程骨架（自 p01 复制纪律）
- [ ] `ParseBehaviorJsonTest` / `RuleScorerTest`（RED 先引用未实现类）
- [ ] `sql/clickhouse_reco_results.sql` + `sql/postgres_reco_items.sql`
- [ ] docker：`p02-init` + `Makefile` `up-p02`
- [ ] 根 README 版本矩阵登记 **jedis 5.2.0**
- [ ] `scripts/verify.sh` / `drill_redis_degrade.sh` / `loadtest.sh` 骨架（`gen_reco_events.py` 在 05-01）

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| baseline 数字填写 | RECO-03 | 须 OrbStack arm64 实测吞吐/lag | 跑 `loadtest.sh` 后确认 `docs/baseline.md` 指标表为本次实测 |
| 简历页措辞 | RECO-03 | 叙事质量 | 核对 `docs/RESUME.md` 动词可指向 verify/baseline/drill-redis 路径 |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 120s（单测）
- [x] `nyquist_compliant: true` set in frontmatter after planner aligns task IDs

**Approval:** pending execution
