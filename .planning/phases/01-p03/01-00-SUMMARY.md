---
phase: 01-p03
plan: 00
subsystem: testing
tags: [flink-cep, junit5, surefire, clickhouse, verify-script, wave0-red]

requires: []
provides:
  - p03 独立 Maven 模块脚手架（surefire + shade + flink-cep）
  - HarshThenFaultPatternTest Wave 0 RED 夹具（within 断言失败）
  - verify.sh 失败态（CH vehicle_alerts count 唯一放行）
  - Makefile package/verify/test 入口
affects: [01-01, 01-02, 01-03]

tech-stack:
  added: [flink-cep 2.2.1, junit-jupiter 5.10.2, maven-surefire-plugin 3.2.5, jackson-databind 2.17.2]
  patterns: [Wave 0 RED fixtures, CH-authoritative verify exit, independent projects/ module]

key-files:
  created:
    - projects/p03-vehicle-monitoring/pom.xml
    - projects/p03-vehicle-monitoring/Makefile
    - projects/p03-vehicle-monitoring/scripts/verify.sh
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/VehicleEvent.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPattern.java
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPatternTest.java
  modified: []

key-decisions:
  - "Wave 0 故意省略 Pattern.within(30s)，单测断言失败以建立 RED 反馈环"
  - "verify.sh 以 ClickHouse count 为唯一 exit 0 条件；Kafka 仅诊断"
  - "p03 独立 pom，不挂入 examples/ 父工程"

patterns-established:
  - "Wave 0 RED: 单测可运行但失败；verify 空环境非 0"
  - "Pattern.getWindowSize() 断言 within 语义，不依赖 MiniCluster"
  - "projects/<id> 独立模块 + 本地 Makefile 入口"

requirements-completed: [VEH-02]

duration: 2min
completed: 2026-07-17
---

# Phase 01 Plan 00: p03 Wave 0 告警夹具 Summary

**独立 p03 Maven 骨架 + HarshThenFaultPatternTest（缺 within → RED）+ CH 权威 verify.sh（空库非 0）**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-17T16:20:32Z
- **Completed:** 2026-07-17T16:22:48Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments

- 落地 `projects/p03-vehicle-monitoring` 独立模块（flink 2.2.1、flink-cep、surefire、shade mainClass 占位）
- `HarshThenFaultPatternTest` 锁定 HARSH_ACCEL/DTC 谓词与 `within(30s)`；当前 Pattern 故意缺 within → 1 fail / 1 pass
- `scripts/verify.sh` 查询固定表 `flinklab.vehicle_alerts`，空环境/无表 exit 1；Makefile 提供 package/verify/test

## Task Commits

Each task was committed atomically:

1. **Task 1: 脚手架 pom + 失败态 HarshThenFaultPatternTest** - `4d77f25` (test)
2. **Task 2: 失败态 verify.sh + 项目 Makefile 桩** - `7ee4ed0` (feat)

**Plan metadata:** （见 final docs commit）

## Files Created/Modified

- `projects/p03-vehicle-monitoring/pom.xml` - 独立模块 + surefire + flink-cep + shade
- `projects/p03-vehicle-monitoring/src/main/java/.../VehicleEvent.java` - vin/signalType/value/eventTime
- `projects/p03-vehicle-monitoring/src/main/java/.../HarshThenFaultPattern.java` - harsh→fault，Wave 0 无 within
- `projects/p03-vehicle-monitoring/src/test/java/.../HarshThenFaultPatternTest.java` - 谓词 + within 断言
- `projects/p03-vehicle-monitoring/scripts/verify.sh` - CH count 唯一放行
- `projects/p03-vehicle-monitoring/Makefile` - package/verify/test；submit/gen 占位

## Decisions Made

- Wave 0 保持 Pattern 不完整（无 within），用可编译可运行的断言失败作为 Nyquist RED，而非编译失败
- verify 失败路径打印 Kafka 诊断说明，但不根据 Kafka 放行
- 凭证复用 docker 既有 `flinklab`/`flinklab123`，不向 projects/ 提交 .env

## Deviations from Plan

None - plan executed exactly as written.

## TDD Gate Compliance

- RED gate: `4d77f25` (`test(01-00): ...`) — `patternRequiresWithinThirtySeconds` 失败
- GREEN gate: **故意跳过** — Wave 0 Nyquist 要求夹具保持 RED；完整 `within(30s)` 留给 01-02

## Known Stubs

| File | Stub | Reason |
|------|------|--------|
| `HarshThenFaultPattern.java` | 缺 `.within(Duration.ofSeconds(30))` | Wave 0 故意 RED；01-02 补齐 |
| `Makefile` submit/gen | `echo 未实现; exit 1` | 下波实现 |
| shade `mainClass` | `VehicleAlertJob` 类尚未创建 | 01-02 管线 |

## Issues Encountered

- 初版测试因 `IterativeCondition<? extends VehicleEvent>.filter` 泛型捕获无法编译；改为 unchecked cast helper 后可运行并在 within 断言处 RED

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Wave 0 夹具齐全，可进入 01-01（compose profile）与 01-02（补 within + VehicleAlertJob）
- 不触碰 docker-compose profile（按计划留给 01-01）

## Self-Check: PASSED

- 全部 6 个产物文件存在
- commits `4d77f25`、`7ee4ed0` 存在于 git log
- surefire / bash -n / 空环境 verify≠0 已验证

---
*Phase: 01-p03*
*Completed: 2026-07-17*
