---
phase: 02-p03-broadcast
plan: "02b"
subsystem: cep-broadcast-wiring
tags: [cep, broadcast-gate, pattern-id, clickhouse, kafka-control, veh-04]

# Dependency graph
requires:
  - phase: 02-p03-broadcast
    provides: PatternActivationGate + PatternControlMessage + PatternIds 三模式工厂
  - phase: 02-02
    provides: 非 keyed BroadcastProcessFunction 门控与默认 HARSH_THEN_FAULT
provides:
  - VehicleAlertJob 静态三 CEP + PatternActivationGate 出口门控 + 双写 gated
  - AlertEvent.patternId 贯通 MATCH/TIMEOUT Handler 族
  - ClickHouse vehicle_alerts.pattern_id DDL/Sink；p03-init vehicle.pattern.control
affects: [02-03 造数切换 e2e / verify PATTERN_ID]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "三 CEP 并行挂载 → union MATCH∪TIMEOUT → connect(broadcast control) → Gate → 双写"
    - "控制面 ParsePatternControlJson 坏消息丢弃；WatermarkStrategy.noWatermarks"
    - "CH INSERT/validate 同步 pattern_id；PatternIds 白名单 + 拒引号反斜杠（T-02-06）"

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/HarshThenFaultHandler.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/TripleHarshHandler.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/DtcPairHandler.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/ParsePatternControlJson.java
  modified:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/JobConfig.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseAlertSink.java
    - projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql
    - docker/docker-compose.yml
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPatternTest.java

key-decisions:
  - "HarshThenFaultHandler 继承 AlertPatternHandler，保留既有单测构造兼容"
  - "三 Handler 共享 AlertPatternHandler.TIMEOUT_TAG，union 后进同一门控（D-09）"
  - "ParsePatternControlJson 作为作业接线必需件随 Task 2 落地（RESEARCH 作业图）"
  - "Sink validate 对 patternId 做 PatternIds 白名单 + 禁引号/反斜杠（T-02-06）"

patterns-established:
  - "出口门控必须在 CEP 之后；禁止 Broadcast 前置过滤事件流"
  - "算子 uid 前缀 p03-；门控 uid=p03-gate-pattern-activation"

requirements-completed: [VEH-04]

# Metrics
duration: 2min
completed: 2026-07-18
---

# Phase 2 Plan 02b: 三 CEP + Broadcast 门控接线 Summary

**VehicleAlertJob 静态并行三 CEP + PatternActivationGate 出口门控可打包；AlertEvent/TIMEOUT/ClickHouse 贯通 pattern_id，p03-init 幂等创建 vehicle.pattern.control（VEH-04 作业半段）。**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-18T01:09:09Z
- **Completed:** 2026-07-18T01:11:28Z
- **Tasks:** 2
- **Files modified:** 12

## Accomplishments

- AlertEvent.patternId 构造贯通；HarshThenFault / TripleHarsh / DtcPair 独立 Handler，MATCH 与 TIMEOUT 均带正确 PatternIds（D-08/D-09）
- VehicleAlertJob：三 CEP uid + control KafkaSource → ParsePatternControlJson → broadcast → PatternActivationGate → 仅 gated 双写
- ClickHouse DDL CREATE+ALTER pattern_id；Sink INSERT/validate；compose p03-init 创建 vehicle.pattern.control；`mvn test` 与 `mvn package` GREEN

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: TIMEOUT patternId 断言** - `4435a30` (test)
2. **Task 1 GREEN: AlertEvent.patternId + 三模式 Handler 族** - `4a267d7` (feat)
3. **Task 2: 三 CEP 作业接线 + pattern_id DDL/Sink + control topic** - `3f35d8e` (feat)

**Plan metadata:** _(见收尾 docs commit)_

## Files Created/Modified

- `cep/HarshThenFaultHandler.java` / `TripleHarshHandler.java` / `DtcPairHandler.java` — 每模式 MATCH/TIMEOUT + patternId
- `cep/AlertPatternHandler.java` / `model/AlertEvent.java` — patternId 贯通；共享 TIMEOUT_TAG
- `ParsePatternControlJson.java` — 控制面 JSON 解析，坏消息丢弃
- `VehicleAlertJob.java` — 三 CEP + Gate + gated 双写
- `JobConfig.java` — `--control-topic` 默认 `vehicle.pattern.control`
- `sink/ClickHouseAlertSink.java` / `sql/clickhouse_alerts.sql` — pattern_id 列
- `docker/docker-compose.yml` — p03-init 幂等建 control topic
- `HarshThenFaultPatternTest.java` — TIMEOUT 断言 patternId

## Decisions Made

- Gate 继续用非 keyed `BroadcastProcessFunction`（沿用 02-02）；作业侧只接线不改门控语义
- Triple/Dtc Handler 仅读各自步骤名（harsh / dtc1·dtc2），避免 Pitfall 7
- ParsePatternControlJson 虽未列在 frontmatter files_modified，属 RESEARCH 作业图必需件（偏差见下）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] 新增 ParsePatternControlJson**
- **Found during:** Task 2（作业接线）
- **Issue:** RESEARCH 作业图需要 control JSON → PatternControlMessage，但 PLAN files_modified 未显式列出该类
- **Fix:** 新增 `ParsePatternControlJson`（坏消息丢弃），并挂入 `p03-parse-pattern-control` uid
- **Files modified:** `ParsePatternControlJson.java`, `VehicleAlertJob.java`
- **Verification:** `mvn -q test && mvn -q package` exit 0
- **Committed in:** `3f35d8e`

**Total deviations:** 1 auto-fixed (Rule 2)
**Impact on plan:** 必要接线件，无范围膨胀；e2e 造数切换仍留给 02-03。

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- 可打包作业在默认激活集下保持 Phase 1 语义（空 Broadcast ≡ HARSH_THEN_FAULT）
- 02-03 可接：`--publish-control` / scenarios / verify.sh 按 `pattern_id` 断言切换

## Self-Check: PASSED

- FOUND: VehicleAlertJob / 三 Handler / ParsePatternControlJson
- FOUND: commits 4435a30, 4a267d7, 3f35d8e

---
*Phase: 02-p03-broadcast*
*Completed: 2026-07-18*
