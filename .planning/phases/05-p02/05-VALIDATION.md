---
phase: 5
slug: p02
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-18
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `05-RESEARCH.md` § Validation Architecture.
> Task IDs to be aligned to PLAN.md files by planner.

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
| 05-*-* | TBD | 0+ | RECO-01 | T-05-01 | 行为 JSON 校验；脏数据丢弃 | unit/smoke | `ParseBehaviorJsonTest`；`docker compose --profile p02 config -q` | ❌ W0 | ⬜ pending |
| 05-*-* | TBD | 0+ | RECO-02 | T-05-02 | Redis key 仅校验 userId 拼接 | unit | `SessionFeatureFunctionTest`；`RuleScorerTest` | ❌ W0 | ⬜ pending |
| 05-*-* | TBD | 0+ | RECO-02 | T-05-01 | CH Sink 拒注入字符；verify SQL 白名单 | e2e | `make match` → `scripts/verify.sh` | ❌ W0 | ⬜ pending |
| 05-*-* | TBD | 0+ | RECO-03 | T-05-03 | Redis 降级仍出 CH 行 | e2e drill | `scripts/drill_redis_degrade.sh` | ❌ W0 | ⬜ pending |
| 05-*-* | TBD | 0+ | RECO-03 | — | loadtest → baseline 非空实测段 | script/doc | `scripts/loadtest.sh`；`docs/baseline.md` | ❌ W0 | ⬜ pending |
| 05-*-* | TBD | 0+ | RECO-03 | — | ADR/ARCHITECTURE/RESUME 存在 | doc | `test -f docs/adr/0001-*.md` 等 | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Planner MUST replace TBD Task IDs with concrete plan task IDs.*

---

## Wave 0 Requirements

- [ ] `projects/p02-realtime-reco/` 工程骨架（自 p01 复制纪律）
- [ ] `ParseBehaviorJsonTest` / `RuleScorerTest`（可选 RED 先引用未实现类）
- [ ] `sql/clickhouse_reco_results.sql` + `sql/postgres_reco_items.sql`
- [ ] docker：`p02-init` + `Makefile` `up-p02`
- [ ] 根 README 版本矩阵登记 **jedis 5.2.0**
- [ ] `scripts/verify.sh` / `gen_reco_events.py` / `drill_redis_degrade.sh` / `loadtest.sh` 骨架

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| baseline 数字填写 | RECO-03 | 须 OrbStack arm64 实测吞吐/lag | 跑 `loadtest.sh` 后人工填入 `docs/baseline.md` 指标表 |
| 简历页措辞 | RECO-03 | 叙事质量 | 核对 `docs/RESUME.md` 动词可指向 verify/baseline 路径 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s（单测）
- [ ] `nyquist_compliant: true` set in frontmatter after planner aligns task IDs

**Approval:** pending
