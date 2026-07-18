---
phase: 06-p5
plan: 00
subsystem: infra
tags: [helm, argo-cd, flink-kubernetes-operator, benchmark, nyquist, orbstack]

# Dependency graph
requires:
  - phase: 05-p02
    provides: P4 三项目压测/演练脚本样板与 OrbStack 实测纪律
provides:
  - Operator Helm chart 1.15.0 与 Argo CD chart 10.1.4 SSOT 登记
  - docs 模块 13/14 编号骨架与权威路径指向
  - check_env.sh（Helm + OrbStack K8s Ready）绿灯
  - PROD-01–04 失败态 harness（matrix / BG / Argo / interview 计数）
affects: [06-01, 06-02, 06-03, 06-04, benchmark, production, interview, monitoring]

# Tech tracking
tech-stack:
  added: [Helm CLI 4.2.3, Flink Operator Helm chart 1.15.0 coords, Argo CD Helm chart 10.1.4]
  patterns: [RED harness 默认非 0, SSOT 先登记再使用, Homebrew helm + 官方 chart 源]

key-files:
  created:
    - docs/13-performance/README.md
    - docs/14-production/README.md
    - production/scripts/check_env.sh
    - benchmark/Makefile
    - benchmark/scripts/run_matrix.sh
    - benchmark/scripts/collect_metrics.py
    - production/scripts/run-bluegreen-drill.sh
    - production/scripts/verify-argocd-sync.sh
    - scripts/count_interview.py
    - monitoring/.gitkeep
  modified:
    - README.md
    - docs/README.md

key-decisions:
  - "Argo CD Helm chart 锁 10.1.4（app v3.4.5），来自 helm search repo argo/argo-cd 当时可解析稳定版"
  - "Operator Helm repo 用 downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/；webhook 关闭时不强制 cert-manager"
  - "矩阵 harness 以 --implemented 门闩保持 RED，直至后续 wave 写出 baseline.md"

patterns-established:
  - "Wave 0 Nyquist：门禁脚本可运行且默认失败，形成 RED→GREEN 反馈环"
  - "生产化脚本统一 set -euo pipefail + FAIL 非 0；禁止假绿"

requirements-completed: []  # Wave 0 仅建立 PROD-01–04 RED harness；完整需求由后续 plan 变绿

# Metrics
duration: 2min
completed: 2026-07-18
---

# Phase 6 Plan 00: P5 Wave 0 Nyquist Summary

**SSOT 登记 Operator/Argo chart、docs 13/14 骨架、Helm+OrbStack Ready 门禁，以及 PROD-01–04 四类可运行失败态 harness**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-18T04:50:29Z
- **Completed:** 2026-07-18T04:52:20Z
- **Tasks:** 2/2
- **Files modified:** 12

## Accomplishments

- 根 README 版本矩阵补全 Flink Kubernetes Operator Helm chart **1.15.0**、Helm CLI **4.2.3**、Argo CD Helm chart **10.1.4**（app v3.4.5）
- `docs/README.md` 模块 13/14 改为带链接登记句；新增最小完成态骨架 README，权威路径指向 `benchmark/` / `production/`
- `production/scripts/check_env.sh` 在本机 OrbStack arm64 实测 exit 0（`ok helm=v4.2.3… k8s=Ready`）；缺失时尝试 `brew install helm`
- 四类 RED harness 默认可运行且非 0：`run_matrix.sh`、`run-bluegreen-drill.sh`、`verify-argocd-sync.sh`、`count_interview.py`（当前题量 30）

## Task Commits

Each task was committed atomically:

1. **Task 1: SSOT 登记 + docs 13/14 + Helm/K8s 门禁** - `7c8aa2e` (chore)
2. **Task 2: PROD-01–04 失败态 harness** - `9e9da78` (feat)

**Plan metadata:** （本 SUMMARY 提交；STATE/ROADMAP 由 orchestrator 更新）

## Files Created/Modified

- `README.md` — Operator Helm / Argo CD chart / Helm CLI SSOT 行
- `docs/README.md` — 模块 13/14 链接登记（进行中）
- `docs/13-performance/README.md` — 性能与压测教材入口骨架
- `docs/14-production/README.md` — 生产化教材入口骨架
- `production/scripts/check_env.sh` — Helm + kubectl Ready 环境门禁
- `benchmark/Makefile` — `matrix` / `dry-run` / `baseline` 目标
- `benchmark/scripts/run_matrix.sh` — 矩阵入口；缺 `--implemented` 则 FAIL
- `benchmark/scripts/collect_metrics.py` — Prom 指标刮取骨架
- `production/scripts/run-bluegreen-drill.sh` — BG CRD 缺失 FAIL
- `production/scripts/verify-argocd-sync.sh` — Argo Application/Synced 缺失 FAIL
- `scripts/count_interview.py` — 题量 &lt;150 exit 1
- `monitoring/.gitkeep` — 看板目录占位（本 Wave 不交 dashboard JSON）

## Decisions Made

- Argo chart 版本以本机 `helm search repo argo/argo-cd` 解析的 **10.1.4** 写死进矩阵，避免「稳定版」漂移
- `check_env.sh` 优先 `kubectl config use-context orbstack`，与 RESEARCH / OrbStack 实测路径一致
- 矩阵 RED 门闩采用显式 `--implemented`，与后续 wave 写出 `benchmark/baseline.md` 对齐

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] 脚本注释去掉 k6/JMeter/Flux 字面量以满足验收 rg**
- **Found during:** Task 2 验收
- **Issue:** 验收要求 `rg -n "k6|JMeter|Flux" benchmark/scripts production/scripts` 无匹配，但纪律注释含这些词
- **Fix:** 改为引用 D-02/D-07 表述，不出现禁用工具名
- **Files modified:** `benchmark/scripts/run_matrix.sh`、`collect_metrics.py`、`benchmark/Makefile`、`verify-argocd-sync.sh`
- **Verification:** rg 无匹配
- **Committed in:** `9e9da78`

---

**Total deviations:** 1 auto-fixed (Rule 3)
**Impact on plan:** 仅注释措辞；行为与 RED 语义不变。

## Issues Encountered

- 本机初始无 Helm；按 plan `user_setup` 执行 `brew install helm`（4.2.3）后 `check_env.sh` 绿灯

## User Setup Required

若其他机器 `brew install helm` 因权限失败：在宿主机手动安装 Helm 3.x/4.x，确保 `command -v helm` 成功，再跑 `bash production/scripts/check_env.sh`。

## Next Phase Readiness

- Wave 0 RED 环已立：后续 06-01+ 可实现矩阵实测、装 Operator、装 Argo、扩 interview，把对应 harness 变绿
- 不在本 plan：完整矩阵、`baseline.md`、Operator 安装、Argo Application、150 题、三块 Grafana JSON

## Self-Check: PASSED

- 全部关键产物文件存在
- 提交 `7c8aa2e`、`9e9da78` 存在于 git log
- `bash production/scripts/check_env.sh` exit 0；四门禁脚本 exit ≠ 0

---
*Phase: 06-p5*
*Completed: 2026-07-18*
