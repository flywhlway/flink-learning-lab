---
phase: 01-p03
plan: 02
subsystem: streaming
tags: [flink-cep, kafka, clickhouse, sinkv2, vehicle-alerts, VEH-02, tdd]

requires:
  - phase: 01-p03-00
    provides: Wave 0 RED 夹具（HarshThenFaultPatternTest / 缺 within）
  - phase: 01-p03-01
    provides: p03 compose profile、vehicle topics、vehicle_alerts DDL
provides:
  - HarshThenFaultPattern within(30s) + AlertPatternHandler TIMEOUT Side Output
  - VehicleAlertJob 可打包 jar（Kafka→CEP→双写）
  - Makefile submit / docker submit-p03 固定 jar 与主类
affects: [01-03]

tech-stack:
  added: []
  patterns: [CEP within+TimedOutPartialMatchHandler, SinkV2 flush-before-clear, 算子 uid+checkpoint, TDD RED→GREEN]

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/JobConfig.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/ParseVehicleJson.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseAlertSink.java
  modified:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPattern.java
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPatternTest.java
    - projects/p03-vehicle-monitoring/Makefile
    - docker/Makefile

key-decisions:
  - "JobConfig 手写 --key 解析（Flink 2.2 无 ParameterTool），默认值对齐 compose 网络"
  - "MATCH/TIMEOUT 经 union 后双写同一 Kafka topic 与 vehicle_alerts 表（alert_type 区分）"
  - "submit 仅复制 p03-vehicle-monitoring-*.jar（排除 original-），flink run -c 固定主类"

patterns-established:
  - "CEP Handler 超时契约可单测：CapturingContext 桩直接调 processTimedOutMatch"
  - "ClickHouseAlertSink：先记 batchSize 再 clear；非 2xx 抛 IOException；vin/alertType 拒引号反斜杠"

requirements-completed: [VEH-02]

duration: 5min
completed: 2026-07-17
---

# Phase 01 Plan 02: p03 CEP 作业半段 Summary

**CEP within(30s)+TIMEOUT Handler 单测 GREEN，VehicleAlertJob 可 shade 打包，Makefile submit / submit-p03 固定 jar 与主类**

## Performance

- **Duration:** 5 min
- **Started:** 2026-07-17T16:28:16Z
- **Completed:** 2026-07-17T16:33:02Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments

- Wave 0 RED 转绿：`HarshThenFaultPattern.within(30s)` + `AlertPatternHandler` TIMEOUT Side Output 契约单测
- `VehicleAlertJob`：Kafka earliest → Parse（白名单）→ WM/idleness → keyBy(vin) → CEP → union 双写 Kafka/ClickHouse；有意义算子均 `.uid(...)`
- `make submit` / `make submit-p03`：仅复制 `p03-vehicle-monitoring-*.jar`，`flink run -c com.flywhl.flinklab.p03.VehicleAlertJob`

## Task Commits

Each task was committed atomically:

1. **Task 1 (RED): TIMEOUT Handler 契约单测** - `a17fc61` (test)
2. **Task 1 (GREEN): CEP within + TIMEOUT Handler** - `a2a0bca` (feat)
3. **Task 2: VehicleAlertJob + Parse/Sink + submit** - `64c4dcb` (feat)

**Plan metadata:** （见 final docs commit）

## Files Created/Modified

- `AlertEvent.java` - MATCH/TIMEOUT 告警 POJO
- `HarshThenFaultPattern.java` - followedBy + within(30s)
- `AlertPatternHandler.java` - PatternProcessFunction + TimedOutPartialMatchHandler
- `HarshThenFaultPatternTest.java` - 谓词 / within / TIMEOUT 契约
- `JobConfig.java` - kafka/CH/checkpoint 默认参数
- `ParseVehicleJson.java` - 脏数据丢弃 + signalType 白名单
- `ClickHouseAlertSink.java` - HTTP SinkV2 → vehicle_alerts
- `VehicleAlertJob.java` - 端到端管线装配
- `projects/.../Makefile` - package/submit
- `docker/Makefile` - submit-p03

## Decisions Made

- Flink 2.2 无 `ParameterTool`，JobConfig 采用 `--key value` / `--key=value` 手写解析，默认值与 plan 一致
- 不调用已迁移的 `ExternalizedCheckpointRetention` API；保留 `enableCheckpointing` + `setCheckpointTimeout`
- MATCH 与 TIMEOUT 经 `union` 共用 Kafka/CH sink，同表 `alert_type` 区分（RESOLVED Q3）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Flink 2.2 无 ParameterTool / ExternalizedCheckpointRetention 包迁移**
- **Found during:** Task 2（VehicleAlertJob 编译）
- **Issue:** 模板沿用的 `ParameterTool` 与 `streaming.api.environment.ExternalizedCheckpointRetention` 在 2.2.1 不可用
- **Fix:** JobConfig 手写参数解析；去掉 externalized retention 设置，保留显式 checkpoint 间隔/超时
- **Files modified:** `JobConfig.java`, `VehicleAlertJob.java`
- **Verification:** `mvn clean package` 成功
- **Committed in:** `64c4dcb` (Task 2)

---

**Total deviations:** 1 auto-fixed (Rule 3)
**Impact on plan:** 仅适配 Flink 2.2 API 漂移，行为与验收标准不变。

## TDD Gate Compliance

- RED gate: `a17fc61` (`test(01-02): ...`) — within + TIMEOUT 断言失败
- GREEN gate: `a2a0bca` (`feat(01-02): ...`) — 单测全部通过

## Known Stubs

| File | Stub | Reason |
|------|------|--------|
| `Makefile` gen | `echo 未实现; exit 1` | 留给 01-03 造数 |
| `scripts/verify.sh` | CH count 权威断言骨架 | 01-03 完善 e2e |

## Issues Encountered

- 编译期发现 Flink 2.2 API 漂移（见 Deviations）；按 Rule 3 就地修复。

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- VEH-02 作业半段完成：可进入 01-03（造数 + e2e verify + 八段式文档）
- 提交前需 OrbStack 上 `make up` + `make up-p03` 后 `make submit` / `submit-p03`

## Self-Check: PASSED

- 产物文件均存在；commits `a17fc61`、`a2a0bca`、`64c4dcb` 在 git log
- `HarshThenFaultPatternTest` exit 0；`mvn clean package` 产出 jar；Makefile 含 submit / submit-p03

---
*Phase: 01-p03*
*Completed: 2026-07-17*
