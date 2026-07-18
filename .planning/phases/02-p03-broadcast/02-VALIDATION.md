---
phase: 2
slug: p03-broadcast
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-18
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `02-RESEARCH.md` ## Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit Jupiter 5.10.2 + maven-surefire-plugin 3.2.5；e2e：bash `verify.sh` |
| **Config file** | `projects/p03-vehicle-monitoring/pom.xml`（surefire） |
| **Quick run command** | `cd projects/p03-vehicle-monitoring && mvn -q test` |
| **Full suite command** | OrbStack：`make up-p03` → `make submit` → control/gen → `bash scripts/verify.sh`（按 pattern_id）；另 `bash scripts/qa_check.sh` |
| **Estimated runtime** | unit ~30s；e2e ~3–5min |

---

## Sampling Rate

- **After every task commit:** Run `cd projects/p03-vehicle-monitoring && mvn -q test`
- **After every plan wave:** 至少一条切换剧本 e2e（TRUNCATE → control → gen → verify PATTERN_ID）
- **Before `/gsd-verify-work`:** 三模式五元组文档 + within 单测绿 + 切换 e2e 绿 + `qa_check.sh` 绿 + 默认路径回归绿
- **Max feedback latency:** unit < 60s；e2e < 10min

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|--------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| W0-01 | 00 | 0 | VEH-03 | — | N/A | unit | `mvn -q -Dtest=PatternRegistryWithinTest test` | ❌ W0 | ⬜ pending |
| W0-02 | 00 | 0 | VEH-03 | — | N/A | unit | `mvn -q -Dtest=TripleHarshPatternTest,DtcPairPatternTest test` | ❌ W0 | ⬜ pending |
| W0-03 | 00 | 0 | VEH-04 | T-02-01 | 仅信任控制 topic 确定性 JSON | unit | `mvn -q -Dtest=PatternActivationGateTest test` | ❌ W0 | ⬜ pending |
| E2E-01 | * | * | VEH-04 | — | N/A | e2e | `PATTERN_ID=TRIPLE_HARSH bash scripts/verify.sh` | ❌ extend | ⬜ pending |
| REG-01 | * | * | ENG | — | N/A | e2e | 不发 control → 默认 `HARSH_THEN_FAULT` verify | ✅ keep | ⬜ pending |
| GATE | * | * | ENG | — | N/A | gate | `bash scripts/qa_check.sh` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `PatternRegistryWithinTest.java` — VEH-03 / D-11（每条 Pattern `getWindowSize()` 非空）
- [ ] `TripleHarshPatternTest.java` / `DtcPairPatternTest.java` — 工厂可构建 + within
- [ ] `PatternActivationGateTest.java` — 默认集 / version 单调 / 过滤
- [ ] `verify.sh` 支持 `PATTERN_ID`（默认 `HARSH_THEN_FAULT`）
- [ ] `gen_vehicle_events.py` scenarios + `--publish-control`

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| PATTERN-LIBRARY.md 五元组评审勾选 | VEH-03 | 文档完整性主观勾选可辅以 `rg` | 打开 `docs/PATTERN-LIBRARY.md`，确认三 ID 均含 within/连接语义/skip/状态上界 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s (unit)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
