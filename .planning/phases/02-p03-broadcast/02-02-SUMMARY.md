---
phase: 02-p03-broadcast
plan: 02
subsystem: cep-broadcast-gate
tags: [broadcast-state, BroadcastProcessFunction, pattern-control, veh-04, version-monotonic]

# Dependency graph
requires:
  - phase: 02-p03-broadcast
    provides: PatternIds 白名单三常量；Wave 0 PatternActivationGateTest RED 夹具
  - phase: 02-01
    provides: PatternRegistry / PATTERN-LIBRARY；pom testExcludes 隔离 Gate 测
provides:
  - PatternControlMessage（activePatterns + version，D-04）
  - PatternActivationGate 非 keyed BroadcastProcessFunction（D-05/D-06；RESOLVED Q1）
  - PatternActivationGateTest GREEN（默认集 / version 单调 / 过滤）
affects: [02-02b VehicleAlertJob 接线, 02-03 e2e control 切换]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "出口门控用非 keyed BroadcastProcessFunction；读写纪律同 e03-C7"
    - "version > stored 才 put；空/空列表 ≡ DEFAULT {HARSH_THEN_FAULT}"
    - "activePatterns 与 PatternIds 白名单求交，原始长度 >3 整条跳过"

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/PatternControlMessage.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/PatternActivationGate.java
  modified:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java
    - projects/p03-vehicle-monitoring/pom.xml
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/PatternActivationGateTest.java

key-decisions:
  - "Gate 采用非 keyed BroadcastProcessFunction（RESEARCH A1 / RESOLVED Q1），不引入 keyBy(vin)"
  - "AlertEvent 仅增加 public patternId 最小字段；Handler/DDL/作业接线留给 02-02b"
  - "原始 activePatterns.size()>3 整条跳过；未知 ID 求交过滤；求交后全未知则跳过"

patterns-established:
  - "门控决策经 resolveActivePatterns / isNewerVersion / isAllowed 包内辅助单测，免 MiniCluster"
  - "MapStateDescriptor 名 p03-active-patterns，state key active"

requirements-completed: []  # VEH-04 门控半段已交付；作业接线/e2e 属 02-02b/02-03，暂不 mark-complete

# Metrics
duration: 1min
completed: 2026-07-18
---

# Phase 2 Plan 02: PatternActivationGate 门控 GREEN Summary

**非 keyed BroadcastProcessFunction 出口门控落地：控制消息模型 + version 单调 + 默认 HARSH_THEN_FAULT，PatternActivationGateTest 转绿（VEH-04 门控半段）。**

## Performance

- **Duration:** 1 min
- **Started:** 2026-07-18T01:06:06Z
- **Completed:** 2026-07-18T01:07:14Z
- **Tasks:** 1
- **Files modified:** 4

## Accomplishments

- 交付 `PatternControlMessage`（`activePatterns` + `version`）对齐 D-04 JSON 契约
- 实现 `PatternActivationGate`：仅 `processBroadcastElement` 写 Broadcast State；空状态默认 `HARSH_THEN_FAULT`；白名单求交与长度上限（T-02-01）
- 移除 pom `testExcludes`，`PatternActivationGateTest` surefire exit 0

## Task Commits

Each task was committed atomically:

1. **Task 1: PatternControlMessage + PatternActivationGate 单测 GREEN** - `aef01a3` (feat)

**Plan metadata:** `7b6c680` (docs: complete plan)

## Files Created/Modified

- `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/PatternControlMessage.java` - 控制面 POJO
- `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/PatternActivationGate.java` - Broadcast 出口门控
- `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java` - 增加 `patternId` 最小字段
- `projects/p03-vehicle-monitoring/pom.xml` - 移除 Gate 测试 `testExcludes`

## Decisions Made

- 非 keyed `BroadcastProcessFunction`（非 `KeyedBroadcastProcessFunction`），满足门控无需 keyed state
- 超长控制消息（raw size > 3）整条跳过，避免截断语义歧义；未知 patternId 求交丢弃
- 不改 `VehicleAlertJob` / DDL / compose（明确留给 02-02b）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] AlertEvent 增加 patternId 最小字段**
- **Found during:** Task 1
- **Issue:** `processElement` 需按 `alert.patternId` 过滤，而既有 `AlertEvent` 无该字段
- **Fix:** 增加 `public String patternId`；未改 Handler 构造/DDL（完整贯通属 02-02b）
- **Files modified:** `AlertEvent.java`
- **Verification:** `mvn -q -Dtest=PatternActivationGateTest test` exit 0
- **Committed in:** `aef01a3`

## Verification

- `mvn -q -Dtest=PatternActivationGateTest test` → exit 0
- Gate 继承 `BroadcastProcessFunction`（非 keyed）；version 比较与默认 `HARSH_THEN_FAULT` 可见
- 无 `System.currentTimeMillis` / `Random` 写入 Broadcast State

## Known Stubs

- `AlertEvent.patternId`：字段已存在，但 Phase 1 Handler / Sink / DDL 尚未赋值与落库（故意留给 02-02b）

## Next Phase Readiness

- 02-02b 可接线：三 CEP union → connect control broadcast → `PatternActivationGate` → 双写
- 控制 topic / `pattern_id` DDL / Handler 填 `patternId` 属 02-02b 范围

## Self-Check: PASSED

- FOUND: PatternActivationGate.java
- FOUND: PatternControlMessage.java
- FOUND: PatternActivationGateTest.java
- FOUND: aef01a3
