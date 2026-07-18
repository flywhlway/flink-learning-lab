---
phase: 02-p03-broadcast
plan: 00
subsystem: testing
tags: [flink-cep, junit, verify, broadcast, pattern-registry, tdd-red]

# Dependency graph
requires:
  - phase: 01-p03
    provides: HarshThenFaultPatternTest 断言风格、verify.sh CH 权威出口、p03 独立 pom
provides:
  - PatternRegistryWithinTest / TripleHarshPatternTest / DtcPairPatternTest RED 夹具（D-11/D-02）
  - PatternActivationGateTest RED 夹具（D-05/D-06/D-07）
  - verify.sh PATTERN_ID 白名单 + MIN_COUNT 骨架（D-08/D-10；T-02-01）
affects: [02-01 PatternRegistry GREEN, 02-02 Gate GREEN, 02-03 e2e switch]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Wave 0 RED：测试引用尚未存在的生产符号，编译失败即反馈环"
    - "verify PATTERN_ID 仅白名单 case 分支拼 SQL，禁止未校验字符串进查询"

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/PatternRegistryWithinTest.java
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/TripleHarshPatternTest.java
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/DtcPairPatternTest.java
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/PatternActivationGateTest.java
  modified:
    - projects/p03-vehicle-monitoring/scripts/verify.sh

key-decisions:
  - "Gate 单测通过 resolveActivePatterns / isNewerVersion / isAllowed 包内辅助方法锁定契约，避免 MiniCluster"
  - "PatternRegistry 测试期望 entry.id()/entry.pattern() 与 PatternIds 三常量"
  - "verify 默认 PATTERN_ID=HARSH_THEN_FAULT、MIN_COUNT=1；非法 PATTERN_ID 立即 exit 1"

patterns-established:
  - "RED 夹具 API 契约：后续 02-01/02-02 按失败符号对齐实现，而非反过来改测试成空壳"
  - "CH 权威出口扩展为 WHERE alert_type=MATCH AND pattern_id=白名单值；Kafka 仍仅 diag"

requirements-completed: []  # Wave 0 仅 RED 夹具；VEH-03/VEH-04 待 02-01+ 转绿后标记

# Metrics
duration: 2min
completed: 2026-07-18
---

# Phase 2 Plan 00: Wave 0 RED 夹具 Summary

**四个 surefire RED 测试类锁定 Registry within / 两新模式 / Gate 决策契约，verify.sh 增加白名单 PATTERN_ID 骨架（生产代码留给后续 Wave）**

## Performance

- **Duration:** 2min
- **Started:** 2026-07-18T00:58:22Z
- **Completed:** 2026-07-18T00:59:47Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- PatternRegistryWithinTest 锁定 size==3 + 每条 `getWindowSize()` 非空 + 三 ID 集合（D-01/D-11）
- TripleHarshPatternTest / DtcPairPatternTest 锁定 within(20s/15s)、HARSH 谓词与 `skipPastLastEvent`（D-02）
- PatternActivationGateTest 锁定默认集 HARSH_THEN_FAULT、version 单调、未激活 patternId 过滤（D-05/D-06/D-07）
- verify.sh 支持 `PATTERN_ID`/`MIN_COUNT` 白名单校验，ClickHouse 仍为唯一 exit 0 条件（D-08/D-10；T-02-01）

## Task Commits

Each task was committed atomically:

1. **Task 1: RED 夹具 — Registry within + 两新模式单测** - `5b5c9a0` (test)
2. **Task 2: RED 夹具 — Gate 单测 + verify PATTERN_ID** - `8a49381` (test)

**Plan metadata:** （见 final docs commit）

_Note: TDD Wave 0 仅 RED 提交；GREEN 在 02-01 / 02-02_

## Files Created/Modified
- `projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/PatternRegistryWithinTest.java` - Registry within 门禁 RED
- `projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/TripleHarshPatternTest.java` - TRIPLE_HARSH within/谓词 RED
- `projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/DtcPairPatternTest.java` - DTC_PAIR within/skip RED
- `projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/PatternActivationGateTest.java` - Gate 决策契约 RED
- `projects/p03-vehicle-monitoring/scripts/verify.sh` - PATTERN_ID 白名单 + pattern_id CH 断言

## Decisions Made
- Gate 测试不启动 MiniCluster，改为断言 `PatternActivationGate.resolveActivePatterns` / `isNewerVersion` / `isAllowed`（02-02 实现时对齐）
- Registry 测试使用 `entry.id()` / `entry.pattern()` 与 `PatternIds.*` 常量（02-01 实现时对齐）
- 未实现 PatternRegistry / 三工厂 / Gate / 作业改线（严格 Wave 0 范围）

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None — 四测均以 testCompile「找不到符号」失败，符合 Wave 0 RED 成功判据。

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- 02-01 可按 RED 符号实现 PatternIds / PatternRegistry / TripleHarshPattern / DtcPairPattern 转绿
- 02-02 可按 Gate 辅助方法契约实现 PatternControlMessage + PatternActivationGate 转绿
- 注意：verify 现按 `pattern_id` 列过滤；DDL/迁移与默认路径回归属后续计划（02-02b/02-03）

## Self-Check: PASSED

- FOUND: PatternRegistryWithinTest.java
- FOUND: TripleHarshPatternTest.java
- FOUND: DtcPairPatternTest.java
- FOUND: PatternActivationGateTest.java
- FOUND: verify.sh (PATTERN_ID)
- FOUND: commit 5b5c9a0
- FOUND: commit 8a49381

---
*Phase: 02-p03-broadcast*
*Completed: 2026-07-18*
