---
phase: 01-p03
plan: 01
subsystem: infra
tags: [docker-compose, profiles, kafka, clickhouse, p03-init, VEH-01]

requires:
  - phase: 01-p03-00
    provides: p03 模块脚手架与 verify.sh 路径约定
provides:
  - p03-init compose profile（vehicle.events / vehicle.alerts + vehicle_alerts DDL）
  - make up-p03 入口（不污染 default make up）
  - smoke_profile.sh 配置级隔离断言
affects: [01-02, 01-03]

tech-stack:
  added: []
  patterns: [compose profiles 隔离 one-shot init, Kafka 镜像 wget CH HTTP DDL, Makefile profile 目标同构 up-ai]

key-files:
  created:
    - projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql
    - projects/p03-vehicle-monitoring/scripts/smoke_profile.sh
  modified:
    - docker/docker-compose.yml
    - docker/Makefile

key-decisions:
  - "p03-init 复用 ${KAFKA_IMAGE}，经 wget POST ClickHouse HTTP 执行挂载 DDL（不引入未登记镜像）"
  - "up-p03 显式 up p03-init；default up 目标不加 --profile p03"

patterns-established:
  - "profile one-shot: restart no + depends_on healthy + 幂等 topic/DDL"
  - "VEH-01 冒烟：config --services 无 profile ≠ 有 profile"

requirements-completed: [VEH-01]

duration: 2min
completed: 2026-07-17
---

# Phase 01 Plan 01: p03 告警链路 compose profile Summary

**独立 `profiles: ["p03"]` one-shot `p03-init` + `make up-p03`，幂等创建 vehicle topics 与 `flinklab.vehicle_alerts`，default `make up` 不受影响**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-17T16:24:08Z
- **Completed:** 2026-07-17T16:26:21Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- 落地 `flinklab.vehicle_alerts` MergeTree DDL（含 `alert_type` MATCH/TIMEOUT）
- compose 新增 `p03-init`（`profiles: ["p03"]`），复用 Kafka 镜像建 topic + wget 调 CH HTTP
- `make up-p03` 与 `smoke_profile.sh` 证明 default 服务列表不含 `p03-init`
- OrbStack 实测：`up-p03` 后 topic `vehicle.events`/`vehicle.alerts` 存在，`EXISTS TABLE flinklab.vehicle_alerts` = 1

## Task Commits

Each task was committed atomically:

1. **Task 1: vehicle_alerts DDL + 失败态 profile 冒烟脚本** - `b4a4626` (feat)
2. **Task 2: compose p03-init + make up-p03（冒烟转绿）** - `422ee4a` (feat)
3. **Task 3: 确认 default make up 不拉起 p03-init** - ⚡ Auto-approved checkpoint（自动化断言 + 运行时 up-p03 实测通过）

**Plan metadata:** （见 final docs commit）

## Files Created/Modified

- `projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql` - vehicle_alerts DDL
- `projects/p03-vehicle-monitoring/scripts/smoke_profile.sh` - profile 隔离配置冒烟
- `docker/docker-compose.yml` - p03-init 服务（profiles p03）
- `docker/Makefile` - up-p03 目标

## Decisions Made

- p03-init 使用 `${KAFKA_IMAGE}` + 内置 `wget` POST ClickHouse HTTP，避免新增未登记第三方镜像（T-1-SC / ENG-01）
- `up-p03` 对齐 `up-ai`：显式 `up -d p03-init`，不修改 `up` 目标语义

## Deviations from Plan

None - plan executed exactly as written.

## Auth Gates

None.

## Known Stubs

None - 本切片无 Flink 作业/造数 stub（留给 01-02 / 01-03）。

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- VEH-01 完成：可进入 01-02（补 Pattern.within + VehicleAlertJob 管线）
- 运行时依赖：核心 kafka/clickhouse 需已起或由 `up-p03` 的 depends_on 拉起

## Self-Check: PASSED

- 产物文件均存在；commits `b4a4626`、`422ee4a` 在 git log
- `smoke_profile.sh` exit 0；compose 双路径 `config -q` 通过
- 运行时：topics + `EXISTS TABLE` = 1 已验证

---
*Phase: 01-p03*
*Completed: 2026-07-17*
