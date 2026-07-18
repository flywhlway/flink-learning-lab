---
phase: 6
slug: p5
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-18
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `06-RESEARCH.md` § Validation Architecture.
> Task IDs aligned to PLAN.md files by planner (2026-07-18).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Bash harness + Maven Surefire（既有 examples/projects）+ `scripts/qa_check.sh` |
| **Config file** | `benchmark/Makefile`；`production/scripts/*.sh`；既有 `projects/*/pom.xml` |
| **Quick run command** | `bash production/scripts/check_env.sh`；`make -C benchmark dry-run`（Wave 0 后）；`python3 scripts/count_interview.py`（&lt;150 非 0） |
| **Full suite command** | `make -C benchmark matrix`（必跑单元格）+ `bash production/scripts/run-bluegreen-drill.sh` + `bash production/scripts/verify-argocd-sync.sh` + `bash scripts/qa_check.sh` + interview 计数 ≥150 + 3× monitoring JSON |
| **Estimated runtime** | 门禁脚本 &lt;30s；dry-run 数分钟；全矩阵/BG 按 OrbStack 实测 |

---

## Sampling Rate

- **After every task commit:** 相关脚本 `bash -n` / dry-run 子集 + 违禁词扫描
- **After every plan wave:** 该波验收命令（下表）全绿
- **Before `/gsd-verify-work`:** Phase gate：`qa_check.sh` + BG 时间线文件 + `benchmark/baseline.md` 实测表 + interview ≥150 + 恰好 3 JSON
- **Max feedback latency:** 门禁 &lt;120s；全矩阵/E2E 另计

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 06-00-01 | 06-00 | 0 | PROD-01..04 / ENG-01 | T-06-01 | SSOT 登记 Operator/Argo chart；无 PyPI CLI | smoke | `bash production/scripts/check_env.sh`；`rg` README 矩阵 | ❌ W0 | ⬜ pending |
| 06-00-02 | 06-00 | 0 | PROD-01..04 | T-06-02 | harness 默认 FAIL 非 0（RED） | script | `bash -n` 各脚本；直接执行 matrix/BG/Argo/count ≠0 | ❌ W0 | ⬜ pending |
| 06-01-01 | 06-01 | 1 | PROD-01 | T-06-03 | 裁剪矩阵 + 无 k6；compose Flink | smoke | `make -C benchmark dry-run` | ❌ W0 | ⬜ pending |
| 06-01-02 | 06-01 | 1 | PROD-01 | T-06-03 | baseline 实测表无 TBD/假数 | e2e/doc | `test -s benchmark/baseline.md`；`rg -n 'TBD\|TODO\|FIXME' benchmark/baseline.md` 空 | ❌ W0 | ⬜ pending |
| 06-02-01 | 06-02 | 1 | PROD-02 | T-06-01/04 | Operator 1.15 CRD + Helm | smoke | `kubectl get crd flinkbluegreendeployments.flink.apache.org` | ❌ W0 | ⬜ pending |
| 06-02-02 | 06-02 | 1 | PROD-02 | T-06-04 | BG 时间线脚本化证据 | e2e | `bash production/scripts/run-bluegreen-drill.sh`；时间线文件非空 | ❌ W0 | ⬜ pending |
| 06-03-01 | 06-03 | 2 | PROD-03 | T-06-05 | Argo Application sync 可判定 | smoke | `bash production/scripts/verify-argocd-sync.sh` | ❌ W0 | ⬜ pending |
| 06-03-02 | 06-03 | 2 | PROD-03 | T-06-05 | GHA workflow 存在可解析 | lint | `test -f .github/workflows/ci.yml` | ❌ W0 | ⬜ pending |
| 06-04-01 | 06-04 | 3 | PROD-04 | T-06-06 | 恰好 3 个 Grafana JSON | lint | `test $(ls monitoring/*.json \| wc -l \| tr -d ' ') -eq 3` | ❌ W0 | ⬜ pending |
| 06-04-02 | 06-04 | 3 | PROD-04 | — | interview ≥150 | lint | `python3 scripts/count_interview.py` exit 0 | ❌ W0 | ⬜ pending |
| 06-04-03 | 06-04 | 3 | PROD-04 | — | best-practice 完整 + docs 13/14 完成态 + qa_check | doc/gate | `bash scripts/qa_check.sh`；docs/PHASES/CHANGELOG 回填 | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Task IDs = `{phase}-{plan}-{task}` matching PLAN.md task order.*

---

## Wave 0 Requirements

- [ ] `production/scripts/check_env.sh`（Helm + kubectl Ready）
- [ ] `benchmark/Makefile` + `benchmark/scripts/run_matrix.sh` + `collect_metrics.py`（默认 FAIL）
- [ ] `production/scripts/run-bluegreen-drill.sh`（无 CRD 时非 0）
- [ ] `production/scripts/verify-argocd-sync.sh`（未 sync 时非 0）
- [ ] `scripts/count_interview.py`（&lt;150 非 0）
- [ ] 根 README SSOT：Operator Helm 1.15.0 + Argo CD chart
- [ ] `docs/13-performance/` + `docs/14-production/` 登记非空

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| baseline 吞吐/lag 数字 | PROD-01 | 须 OrbStack arm64 实测 | 跑 `make -C benchmark matrix` 后确认 `benchmark/baseline.md` 指标表为本机实测 |
| BG 时间线可读性 | PROD-02 | 叙事/值班可读 | 打开 `production/docs/bluegreen-timeline.md`（或计划约定路径）核对事件顺序 |
| Grafana 面板肉眼打开 | PROD-04 | UI 确认 | Grafana 导入/provisioning 后打开三块看板 |

---

## Threat → Control Map (from RESEARCH Security Domain)

| ID | Pattern | Mitigation in plans |
|----|---------|---------------------|
| T-06-01 | 供应链假 CLI（PyPI helm） | 仅 Homebrew + 官方 Helm chart（06-00） |
| T-06-02 | 假绿门禁 | Wave 0 RED harness（06-00） |
| T-06-03 | 矩阵假完成/假数 | SKIPPED+原因；禁 TBD（06-01） |
| T-06-04 | 纸面 Blue/Green | 脚本化时间线非 0（06-02） |
| T-06-05 | GitOps 散文 / 误 sync | verify-argocd + ns 隔离（06-03） |
| T-06-06 | 臆造 PromQL / 不可导入 JSON | 复用值班五指标 + 导入校验（06-04） |

---

## Validation Sign-Off

- [x] All plans have automated verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers MISSING harness references
- [x] No watch-mode flags
- [x] Feedback latency &lt; 120s（门禁脚本）
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending execution
