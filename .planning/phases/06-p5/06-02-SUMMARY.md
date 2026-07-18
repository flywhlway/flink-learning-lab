---
phase: 06-p5
plan: 02
subsystem: infra
tags: [flink-kubernetes-operator, blue-green, helm, orbstack, p03, kafka]

# Dependency graph
requires:
  - phase: 06-p5
    provides: Wave 0 check_env + RED run-bluegreen-drill harness；PROD-01 baseline 不共享文件
  - phase: 03-p03 / p03 project
    provides: VehicleAlertJob shade jar 与 topic/作业参数约定
provides:
  - Operator 1.15.0 Helm 安装（flink-operator ns，webhook 关闭）
  - p03 FlinkBlueGreenDeployment chart + 本地 Application 镜像
  - 可观察 BG 时间线 production/docs/bluegreen-timeline.md（脚本产物）
  - operator-install / bluegreen-sop / production README 可复现入口
affects: [06-03, 06-04, production, PROD-02]

# Tech tracking
tech-stack:
  added: [Flink Kubernetes Operator 1.15.0, production/charts/p03-vehicle-alert, Kafka K8S listener :9095]
  patterns: [混合拓扑 host.docker.internal:9095, FlinkBlueGreenDeployment 状态机时间线, chart 内 flink SA RBAC]

key-files:
  created:
    - production/scripts/install-operator.sh
    - production/scripts/probe_kafka_from_k8s.sh
    - production/charts/p03-vehicle-alert/Chart.yaml
    - production/charts/p03-vehicle-alert/values.yaml
    - production/charts/p03-vehicle-alert/templates/flink-bluegreen.yaml
    - production/charts/p03-vehicle-alert/templates/rbac.yaml
    - production/docker/p03-k8s-image/Dockerfile
    - production/docs/operator-install.md
    - production/docs/bluegreen-sop.md
    - production/docs/bluegreen-timeline.md
  modified:
    - docker/docker-compose.yml
    - production/scripts/run-bluegreen-drill.sh
    - production/README.md

key-decisions:
  - "Kafka 增加 K8S listener 9095（advertised host.docker.internal:9095）；拒绝 hostNetwork（单节点 JM+TM 端口冲突）"
  - "chart 自带 flink SA Role/RoleBinding（Operator 默认仅在 flink-operator ns 建 Role）"
  - "演练触发字段：image.tag dev→dev-green；upgradeMode=savepoint 与 SAVEPOINTING_* 协同"

patterns-established:
  - "PROD-02 硬门禁=FlinkBlueGreenDeployment 状态机 + 脚本时间线，非 upgradeMode 散文"
  - "混合拓扑探针先 TCP 再 Kafka 协议，锁定唯一 bootstrap 写入 values"

requirements-completed: [PROD-02]

# Metrics
duration: 26min
completed: 2026-07-18
---

# Phase 6 Plan 02: Operator Blue/Green Summary

**Helm 安装 Flink Kubernetes Operator 1.15.0，并以 p03 VehicleAlertJob 的 FlinkBlueGreenDeployment 在 OrbStack 上跑通 ACTIVE_BLUE→ACTIVE_GREEN 可观察演练时间线**

## Performance

- **Duration:** 26 min
- **Started:** 2026-07-18T05:36:46Z
- **Completed:** 2026-07-18T06:02:53Z
- **Tasks:** 2/2
- **Files modified:** 12

## Accomplishments

- Operator **1.15.0** 经 Helm 安装于 `flink-operator`（`webhook.create=false`）；CRD `flinkbluegreendeployments.flink.apache.org` 存在；镜像 `ghcr.io/apache/flink-kubernetes-operator:1.15.0`
- p03 chart 部署后作业 `RUNNING`/`STABLE`；`run-bluegreen-drill.sh` **exit 0**，时间线记录 `ACTIVE_BLUE` → `SAVEPOINTING_BLUE` → `TRANSITIONING_TO_GREEN` → `ACTIVE_GREEN`
- 锁定 Kafka bootstrap=`host.docker.internal:9095`（compose K8S listener）；SOP + `production/README.md` 可一键复现

## Task Commits

Each task was committed atomically:

1. **Task 1: Helm 安装 Operator 1.15 + K8s↔Kafka 探针 + p03 FlinkBlueGreenDeployment chart** - `696cdda` (feat)
2. **Task 2: 脚本化 Blue/Green 演练产出时间线证据 + SOP** - `1ffd6b4` (feat)

**Plan metadata:** （本 SUMMARY 提交后由 gsd-sdk / final commit 记录）

## Files Created/Modified

- `docker/docker-compose.yml` — Kafka 增加 K8S listener `9095` / advertised `host.docker.internal:9095`
- `production/scripts/install-operator.sh` — Operator 1.15.0 Helm upgrade --install + CRD 断言
- `production/scripts/probe_kafka_from_k8s.sh` — TCP + Kafka 协议探针，锁定 bootstrap
- `production/scripts/run-bluegreen-drill.sh` — RED→GREEN：完整 TRANSITION + timeline 写入
- `production/charts/p03-vehicle-alert/**` — FlinkBlueGreenDeployment + RBAC
- `production/docker/p03-k8s-image/Dockerfile` — `flink:2.2.1-java21` + p03 shade jar
- `production/docs/operator-install.md` / `bluegreen-sop.md` / `bluegreen-timeline.md`
- `production/README.md` — PROD-02 可复现总入口（GitOps 留给 06-03）

## Decisions Made

- 不用 `hostNetwork`：EXTERNAL `:9094` 协议因 advertised=`localhost` 失败；单节点 hostNetwork 又会让 JM/TM 抢端口 → 增加 compose **K8S:9095** listener
- Flink SA RBAC 放进作业 chart（Rule 2）：缺 Role 时 JM CrashLoop（cannot list pods）
- 演练前 `ensure_topics`：compose 关闭 auto-create，缺 topic 会导致 Source Coordinator 全局失败

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Critical] Kafka K8S listener + compose 端口 9095**
- **Found during:** Task 1（探针）
- **Issue:** `host.docker.internal:9094` TCP 通但 Kafka 协议因 advertised `localhost:9094` 失败；hostNetwork 不适合 JM+TM
- **Fix:** `docker-compose.yml` 增加 `K8S://0.0.0.0:9095` / advertised `host.docker.internal:9095`；values 锁定该 bootstrap
- **Files modified:** `docker/docker-compose.yml`, `values.yaml`, `probe_kafka_from_k8s.sh`, `operator-install.md`
- **Verification:** `probe_kafka_from_k8s.sh` exit 0；作业 RUNNING
- **Committed in:** `696cdda`

**2. [Rule 2 - Critical] chart 内 flink SA Role/RoleBinding**
- **Found during:** Task 2（首次部署 CrashLoop）
- **Issue:** Operator 仅在 `flink-operator` ns 创建 flink Role；作业 ns `flink` 的 SA 无 pods list 权限
- **Fix:** `templates/rbac.yaml`（SA + Role + RoleBinding）
- **Files modified:** `production/charts/p03-vehicle-alert/templates/rbac.yaml`
- **Verification:** `kubectl auth can-i list pods --as=system:serviceaccount:flink:flink -n flink` → yes；ACTIVE_BLUE RUNNING
- **Committed in:** `1ffd6b4`

**3. [Rule 1 - Bug] 演练脚本确保 topic + Kafka 运行检测**
- **Found during:** Task 2（JobException / drill 假阴性）
- **Issue:** topic 缺失导致 Source Coordinator fail；`docker compose ps --status running` 检测不可靠
- **Fix:** `ensure_topics` + 放宽 compose/ps 检测
- **Files modified:** `run-bluegreen-drill.sh`, `operator-install.md`
- **Verification:** drill exit 0，时间线含完整状态迁移
- **Committed in:** `1ffd6b4`

---

**Total deviations:** 3 auto-fixed (2× Rule 2, 1× Rule 1)
**Impact on plan:** 均为混合拓扑 / RBAC / topic 正确性所必需；未扩大到 Argo/题库范围。

## Issues Encountered

- 首次部署因缺 RBAC CrashLoop；abort grace 到期后进入 FAILING，清理后重装恢复
- Kafka 重建后 topic 丢失（auto.create=false）→ 在 drill/install 文档中强制 create --if-not-exists

## User Setup Required

None — 全程本机 OrbStack + 已有 compose 基座；无外部云凭证。

## Known Stubs

None — drill 已绿；timeline 为实测 kubectl 片段，无 TODO/占位。

## Threat Flags

None — 未新增计划外网络入口；演练限 `flink` / `flink-operator` ns；Helm 坐标仍为 Apache 1.15.0 SSOT。

## Next Phase Readiness

- PROD-02 硬门禁已满足；06-03 可安装 Argo CD 并将本 chart 纳入 Application（勿改 BG 演练路径）
- 勿覆盖 `benchmark/baseline.md`（06-01）

## Self-Check: PASSED

- FOUND: production/scripts/install-operator.sh
- FOUND: production/scripts/run-bluegreen-drill.sh
- FOUND: production/docs/bluegreen-timeline.md
- FOUND: production/charts/p03-vehicle-alert/templates/flink-bluegreen.yaml
- FOUND: commits `696cdda`, `1ffd6b4`
- Operator image confirmed: `ghcr.io/apache/flink-kubernetes-operator:1.15.0`
- Drill: exit 0；timeline path `production/docs/bluegreen-timeline.md`
