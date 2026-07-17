---
phase: 1
slug: p03
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-17
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | bash 断言脚本（主）+ JUnit Jupiter 5.10.2 / flink-test-utils 2.2.1（辅） |
| **Config file** | `projects/p03-vehicle-monitoring/pom.xml`（Wave 0 启用 surefire） |
| **Quick run command** | `cd projects/p03-vehicle-monitoring && mvn -q -Dtest=HarshThenFaultPatternTest test` |
| **Full suite command** | `projects/p03-vehicle-monitoring/scripts/verify.sh`（须先 submit + gen） |
| **Estimated runtime** | 单测/package ~30–60s；e2e verify ~3–5min（含 compose/作业，独立 Plan 03 采样） |

---

## Sampling Rate

- **After every task commit:** Run `mvn -q test`（若有单测）+ `cd docker && docker compose config -q && docker compose --profile p03 config -q`
- **After Plan 02 wave:** 快反馈 — `mvn -q test && mvn -q package`（不跑完整 e2e）
- **After Plan 03 wave:** `make package && make submit && make gen` + 等待 + `scripts/verify.sh`（OrbStack 集群已 up）
- **Before `/gsd-verify-work`:** Full suite must be green + `bash scripts/qa_check.sh`
- **Max feedback latency:** 快路径 &lt;60s；e2e &lt;300s（独立采样）

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-00-01 | 00 | 0 | VEH-02 | T-1-01 | JSON 解析失败不进入 CEP | unit | `mvn -q -Dtest=HarshThenFaultPatternTest test` | ❌ W0 | ⬜ pending |
| 01-00-02 | 00 | 0 | VEH-02 | — | verify 空库非 0 | smoke | `bash scripts/verify.sh` 期望 ≠0 | ❌ W0 | ⬜ pending |
| 01-01-01 | 01 | 1 | VEH-01 | — | N/A | smoke | `bash scripts/smoke_profile.sh` / compose config | ❌ W0 | ⬜ pending |
| 01-01-02 | 01 | 1 | VEH-01 | T-1-01 | DDL 静态文件 | smoke | `docker compose --profile p03 config -q` | ❌ W0 | ⬜ pending |
| 01-02-01 | 02 | 2 | VEH-02 | T-1-02 | within 强制 | unit | `mvn -q -Dtest=HarshThenFaultPatternTest test` | ❌ W0 | ⬜ pending |
| 01-02-02 | 02 | 2 | VEH-02 | T-1-01 / T-1-05 | 脏事件过滤；固定 jar submit | unit/build | `mvn -q package` + grep `.uid(` | ❌ W0 | ⬜ pending |
| 01-03-01 | 03 | 3 | VEH-02 | T-1-01 | 断言型验收 | e2e | `make package && make submit && make gen` + wait + `verify.sh`；空库负例 ≠0 | ❌ W0 | ⬜ pending |
| 01-03-02 | 03 | 3 | VEH-02 | T-1-04 | 文档无新增密钥 | docs | README 八段关键词断言 | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `projects/p03-vehicle-monitoring/scripts/verify.sh` — 覆盖 VEH-02 断言与失败退出码
- [ ] `projects/p03-vehicle-monitoring/src/test/java/.../HarshThenFaultPatternTest.java` — 覆盖模式条件 / within 语义
- [ ] `projects/p03-vehicle-monitoring/pom.xml` — 启用 `maven-surefire-plugin`
- [ ] `docker compose --profile p03 config -q` 纳入文档/脚本检查路径（`smoke_profile.sh`）

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| default `make up` 不拉起 p03-init | VEH-01 | 需观察容器列表与 compose 依赖图 | `make up` 后 `docker compose ps` 确认无 p03-init；再 `docker compose --profile p03 up -d` 验证 profile |
| CH/Kafka 告警可观察 | VEH-02 | 目视确认 MATCH 行与体验路径 | Plan 03 checkpoint：submit+gen 后查 CH/Kafka；空表 verify 非 0 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Fast path feedback &lt;60s；e2e &lt;300s（分 Plan 采样）
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
