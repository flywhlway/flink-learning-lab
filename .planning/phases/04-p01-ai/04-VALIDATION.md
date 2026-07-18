---
phase: 4
slug: p01-ai
status: drafted
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-18
updated: 2026-07-18
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `04-RESEARCH.md` § Validation Architecture.
> Task IDs aligned to PLAN.md files by planner (2026-07-18).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 5.10.2（对齐 p03） |
| **Config file** | `projects/p01-log-ai-platform/pom.xml` surefire（新建） |
| **Quick run command** | `cd projects/p01-log-ai-platform && mvn -q -Dtest=RuleTaggerTest,ParseLogJsonTest,BudgetGateTest test` |
| **Full suite command** | `cd projects/p01-log-ai-platform && mvn -q test` |
| **Estimated runtime** | ~30–90 seconds（单测）；E2E 另计 |

---

## Sampling Rate

- **After every task commit:** Run quick run command（或 `mvn -q test`）
- **After every plan wave:** `mvn -q test` + `bash -n scripts/*.sh`
- **Before `/gsd-verify-work`:** Full suite green + `make verify`（OrbStack）
- **Max feedback latency:** 120 seconds（单测）；E2E 按文档

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 04-00-T1 | 00 | 0 | LOG-01–05 | T-04-05 | 主 pom 禁 Agents/Milvus | unit RED | `mvn -q -Dtest=ParseLogJsonTest,RuleTaggerTest,BudgetGateTest test` expect ≠0 | ❌ W0 | ⬜ pending |
| 04-00-T2 | 00 | 0 | LOG-01–05 | T-04-01 | verify SQL 白名单骨架 | script RED | `bash -n scripts/*.sh` + scripts exit ≠0 | ❌ W0 | ⬜ pending |
| 04-00-T3 | 00 | 0 | LOG-01 | T-04-06 | p01 profile 隔离钩子 | compose | `docker compose config -q`; up-p01 存在 | ❌ W0 | ⬜ pending |
| 04-01-T1 | 01 | 1 | LOG-01 | T-04-01 | JSON 脏数据丢弃 | unit | `mvn -q -Dtest=ParseLogJsonTest test` | ❌ W0 | ⬜ pending |
| 04-01-T2 | 01 | 1 | LOG-01 | T-04-05 | shade jar；默认 AI off | build | `mvn -q clean package -DskipTests` | ❌ W0 | ⬜ pending |
| 04-01-T3 | 01 | 1 | LOG-01 | — | profile 隔离冒烟 | smoke | `bash scripts/smoke_p01_profile.sh` | ❌ W0 | ⬜ pending |
| 04-02-T1 | 02 | 2 | LOG-01, LOG-02 | T-04-01 | CH Sink 拒注入字符 | unit/build | `mvn -q -Dtest=RuleTaggerTest,ParseLogJsonTest test` | ❌ W0 | ⬜ pending |
| 04-02-T2 | 02 | 2 | LOG-01, LOG-02 | T-04-01 | CH 权威 verify | e2e | `bash scripts/verify.sh` after submit+gen | ❌ W0 | ⬜ pending |
| 04-02-T3 | 02 | 2 | LOG-01, LOG-02 | — | README 无违禁词 | doc | grep README 启动/验证节 | ❌ W0 | ⬜ pending |
| 04-03-T1 | 03 | 3 | LOG-03 | T-04-02, T-04-03 | AI 超时降级；默认 AI off | build | package + grep Async/api/chat | ❌ W0 | ⬜ pending |
| 04-03-T2 | 03 | 3 | LOG-03 | T-04-01 | verify-ai 白名单 | e2e opt | `bash scripts/verify_ai.sh` | ❌ W0 | ⬜ pending |
| 04-03-T3 | 03 | 3 | LOG-03 | T-04-02 | 降级清单四格 | doc | DEGRADE-CHECKLIST.md exists | ❌ W0 | ⬜ pending |
| 04-04-T1 | 04 | 4 | LOG-04 | T-04-03 | 预算熔断可观察 | unit | `mvn -q -Dtest=BudgetGateTest test` | ❌ W0 | ⬜ pending |
| 04-04-T2 | 04 | 4 | LOG-04 | T-04-02 | 护栏 BLOCK | build | package + GuardrailFunction | ❌ W0 | ⬜ pending |
| 04-04-T3 | 04 | 4 | LOG-04 | T-04-05 | Prom/日志观察命令 | doc | README 含 9249/9090 + counters | ❌ W0 | ⬜ pending |
| 04-05-T1 | 05 | 5 | LOG-05 | T-04-03 | loadtest/drill 非 stub | script | loadtest + drill + baseline.md | ❌ W0 | ⬜ pending |
| 04-05-T2 | 05 | 5 | LOG-05 | T-04-01 | ADR/ARCHITECTURE/RESUME | doc | file exists gates | ❌ W0 | ⬜ pending |
| 04-05-T3 | 05 | 5 | LOG-05 | — | 15-01 + qa_check | gate | `bash scripts/qa_check.sh` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `projects/p01-log-ai-platform/` 工程树（pom + 包结构）
- [ ] `ParseLogJsonTest.java` — LOG-01/02
- [ ] `RuleTaggerTest.java` — LOG-02
- [ ] `BudgetGateTest.java` — LOG-04（纯函数，无 MiniCluster）
- [ ] `scripts/verify.sh` / `verify_ai.sh` / `drill_ai_degrade.sh` / `loadtest.sh` 骨架
- [ ] docker `p01-init` + `make up-p01` 钩子
- [ ] 单测不强制 MiniCluster/Testcontainers

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Ollama 模型名与本机 `ollama list` 对齐 | LOG-03 | 本机模型漂移 | 文档覆写 `--ai.model` 或 `ollama pull`；`verify-ai` 前核对 |
| Prometheus 自定义 Counter 最终指标名 | LOG-04 | reporter 标签改写需实测 | 首次 submit 后从 `:9249`/Prom 回填 README |
| Grafana 浏览器打开（若有 dashboard） | LOG-04/05 | 可选 UI | 可选；不挡 `verify` |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 120s for unit path
- [x] `nyquist_compliant: true` set in frontmatter after plans land

**Approval:** pending execution
