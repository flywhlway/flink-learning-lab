---
phase: 02-p03-broadcast
plan: 01
subsystem: cep-pattern-library
tags: [flink-cep, pattern-registry, within, skipPastLastEvent, veh-03]

# Dependency graph
requires:
  - phase: 02-p03-broadcast
    provides: Wave 0 RED 夹具 PatternRegistryWithinTest / TripleHarshPatternTest / DtcPairPatternTest
  - phase: 01-p03
    provides: HarshThenFaultPattern + HarshThenFaultPatternTest 基线
provides:
  - PatternIds / PatternRegistry 恰好 3 条预编译模式（D-01）
  - TripleHarshPattern times(3).consecutive+within(20s)；DtcPairPattern skipPastLastEvent+within(15s)（D-02）
  - PATTERN-LIBRARY.md 五元组 + 评审 checklist（D-03）；within 门禁单测 GREEN（D-11）
affects: [02-02 PatternActivationGate, 02-02b DDL pattern_id, 02-03 e2e switch]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "PatternRegistry.all() 仅聚合工厂，不解析 JSON、不接触 Kafka"
    - "skip 必须挂 begin：Pattern.begin(name, AfterMatchSkipStrategy.skipPastLastEvent())"
    - "Wave 0 Gate RED 测试在 02-01 期间用 compiler testExcludes 隔离，02-02 移除"

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/PatternIds.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/PatternRegistry.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/TripleHarshPattern.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/DtcPairPattern.java
    - projects/p03-vehicle-monitoring/docs/PATTERN-LIBRARY.md
  modified:
    - projects/p03-vehicle-monitoring/pom.xml

key-decisions:
  - "Registry Entry 用 record(id, pattern) 对齐 Wave 0 entry.id()/entry.pattern() 契约"
  - "within 秒数锁定 CONTEXT 默认 30/20/15，未做 Discretion 微调"
  - "排除 PatternActivationGateTest 编译直至 02-02 交付 Gate/ControlMessage"

patterns-established:
  - "模式库合入门禁：PatternRegistryWithinTest size==3 + getWindowSize 非空"
  - "PATTERN-LIBRARY 五元组表格 + checklist：缺项即失败"

requirements-completed: [VEH-03]

# Metrics
duration: 2min
completed: 2026-07-18
---

# Phase 2 Plan 01: 模式库三工厂 + PATTERN-LIBRARY Summary

**PatternIds/Registry 与 TRIPLE_HARSH/DTC_PAIR 工厂落地，within 门禁四测 GREEN；PATTERN-LIBRARY.md 三行五元组可评审（VEH-03）**

## Performance

- **Duration:** 2min
- **Started:** 2026-07-18T01:02:03Z
- **Completed:** 2026-07-18T01:03:57Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- `PatternRegistry.all()` 恰好 3 条：`HARSH_THEN_FAULT` / `TRIPLE_HARSH` / `DTC_PAIR`，每条 `getWindowSize()` 非空（D-01/D-11）
- `TripleHarshPattern`：`times(3).consecutive()` + `within(20s)`；`DtcPairPattern`：`skipPastLastEvent` 挂 begin + `within(15s)`（D-02）
- `PATTERN-LIBRARY.md` 五元组表格 + 评审 checklist；写明门控关闭 ≠ CEP 状态停止（D-03 / Pitfall 4）
- 未改 VehicleAlertJob / Broadcast / DDL / 造数（范围留给 02-02/02-03）

## Task Commits

Each task was committed atomically:

1. **Task 1: 三工厂 + PatternRegistry 使 within 单测 GREEN** - `e67b099` (feat)
2. **Task 2: PATTERN-LIBRARY.md 五元组与评审清单** - `5201017` (docs)

**Plan metadata:** _(见下方 final docs commit)_

_Note: TDD Wave 0 RED 在 02-00；本计划为 GREEN 实现_

## Files Created/Modified
- `projects/p03-vehicle-monitoring/src/main/java/.../cep/PatternIds.java` - 三常量 + `isKnown` 白名单
- `projects/p03-vehicle-monitoring/src/main/java/.../cep/PatternRegistry.java` - `Entry` record + `all()`
- `projects/p03-vehicle-monitoring/src/main/java/.../cep/TripleHarshPattern.java` - consecutive 急加速突发
- `projects/p03-vehicle-monitoring/src/main/java/.../cep/DtcPairPattern.java` - DTC 对 + skipPastLastEvent
- `projects/p03-vehicle-monitoring/docs/PATTERN-LIBRARY.md` - 五元组表 + checklist
- `projects/p03-vehicle-monitoring/pom.xml` - 排除 Gate RED 测试编译（02-02 前）

## Decisions Made
- Registry 使用 `record Entry(String id, Pattern<...> pattern)`，与 Wave 0 测试 `entry.id()` / `entry.pattern()` 对齐
- within 秒数保持 CONTEXT 默认 30/20/15，与五元组文档一致
- 不实现 Broadcast Gate；用 `maven-compiler-plugin` `testExcludes` 隔离 `PatternActivationGateTest` 直至 02-02

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Wave 0 Gate RED 测试阻塞 testCompile**
- **Found during:** Task 1（运行 surefire 验证）
- **Issue:** `PatternActivationGateTest` 引用尚未交付的 `PatternControlMessage` / `PatternActivationGate`（属 02-02），导致全模块 `testCompile` 失败，无法跑通本计划四测
- **Fix:** 在 `pom.xml` 对 `PatternActivationGateTest.java` 配置 `testExcludes`，并加注释标明 02-02 移除
- **Files modified:** `projects/p03-vehicle-monitoring/pom.xml`
- **Verification:** `mvn -q -Dtest=PatternRegistryWithinTest,TripleHarshPatternTest,DtcPairPatternTest,HarshThenFaultPatternTest test` exit 0
- **Committed in:** `e67b099`（Task 1）

## Issues Encountered
None beyond the Gate testCompile block above（已按 Rule 3 处理）。

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- 02-02 可实现 `PatternControlMessage` + `PatternActivationGate`，并**移除** pom 中对 `PatternActivationGateTest` 的 `testExcludes`
- 02-02/02-03 再改作业图、DDL `pattern_id`、造数 scenario 与 e2e 切换

## Self-Check: PASSED

- FOUND: PatternIds.java / PatternRegistry.java / TripleHarshPattern.java / DtcPairPattern.java
- FOUND: PATTERN-LIBRARY.md
- FOUND: commit e67b099
- FOUND: commit 5201017

---
*Phase: 02-p03-broadcast*
*Completed: 2026-07-18*
