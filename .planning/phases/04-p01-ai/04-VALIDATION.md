---
phase: 4
slug: p01-ai
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-18
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `04-RESEARCH.md` § Validation Architecture.

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
| TBD | 00 | 0 | LOG-* | — | Wave 0 RED stubs | unit | `mvn -q test` (expect fail→scaffold) | ❌ W0 | ⬜ pending |
| TBD | V1 | 1 | LOG-01 | T-04-01 | JSON 脏数据丢弃 | unit/e2e | ParseLogJsonTest; `make verify` prep | ❌ W0 | ⬜ pending |
| TBD | V2 | 2 | LOG-01, LOG-02 | T-04-01 | CH Sink 拒注入字符 | e2e | `bash scripts/verify.sh` | ❌ W0 | ⬜ pending |
| TBD | V3 | 3 | LOG-03 | T-04-02 | AI 超时降级；默认 AI off | e2e opt | `bash scripts/verify_ai.sh` | ❌ W0 | ⬜ pending |
| TBD | V4 | 4 | LOG-04 | T-04-03 | 预算熔断可观察 | unit | BudgetGateTest; Prom/日志命令 | ❌ W0 | ⬜ pending |
| TBD | V5 | 5 | LOG-05 | — | 文档/脚本门禁 | script | loadtest; drill; file exists | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Task IDs filled by planner when PLAN.md files exist.*

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

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s for unit path
- [ ] `nyquist_compliant: true` set in frontmatter after plans land

**Approval:** pending
