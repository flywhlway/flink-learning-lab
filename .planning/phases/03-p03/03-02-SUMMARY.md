---
phase: 03-p03
plan: 02
subsystem: ops
tags: [flink, loadtest, watermark, prometheus, veh-06, baseline, drill]

requires:
  - phase: 03-p03-01
    provides: p03_vehicle_alert + p03_vehicle_window_metrics 作业、Grafana/Prom 可观测基座
provides:
  - gen_vehicle_events.py --rate/--duration/--frozen-event-time/--partial
  - loadtest.sh → docs/baseline.md（OrbStack 实测）
  - drill_watermark_stall.sh stall→recover（冻结 HEARTBEAT）
  - Makefile loadtest / drill-watermark 一键入口
affects: [03-03, VEH-06, VEH-07]

tech-stack:
  added: []
  patterns: [冻结 eventTime HEARTBEAT 停滞、CEP currentInputWatermark 推算 lag、无 k6 造数压测]

key-files:
  created:
    - projects/p03-vehicle-monitoring/docs/baseline.md
  modified:
    - projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py
    - projects/p03-vehicle-monitoring/scripts/loadtest.sh
    - projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh
    - projects/p03-vehicle-monitoring/Makefile
    - projects/p03-vehicle-monitoring/README.md

key-decisions:
  - "压测默认 100 eps × 120s、热身 45s 丢弃（D-08）"
  - "停滞断言以 CEP currentInputWatermark + 墙钟推算 lag 为主；Source currentEmitEventTimeLag 仅诊断"
  - "冻结模式支持 vin-count 覆盖多分区，禁止仅靠 idle 分区"

patterns-established:
  - "Pattern: loadtest 刮 PromQL job_name=~p03.* 写 baseline 四段结构"
  - "Pattern: drill stall=partial harsh-open + frozen HEARTBEAT≥45s → recover verify.sh"

requirements-completed: [VEH-06]

duration: 22min
completed: 2026-07-18
---

# Phase 3 Plan 02: VEH-06 loadtest + watermark stall Summary

**扩展 gen --rate/--duration/--frozen-event-time，OrbStack 实测写出 docs/baseline.md（100 eps×120s），并以冻结 HEARTBEAT 完成 watermark 停滞→恢复 MATCH 演练。**

## Performance

- **Duration:** ~22 min
- **Started:** 2026-07-18T02:04:45Z
- **Completed:** 2026-07-18T02:26:48Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments

- `gen_vehicle_events.py` 支持 `--rate`/`--eps` + `--duration` 恒定负载、`--frozen-event-time`/`--freeze-at`/`--partial harsh-open`（无 k6/JMeter）
- `make loadtest` 在 OrbStack 跑通并写入含环境/负载/指标/结论的 `docs/baseline.md`（实测 ckpt max 81ms、restarts 0、produce 100 eps）
- `make drill-watermark` stall→recover exit 0：CEP WM 停滞（wm_delta=0）、推算 lag 增长约 53s、恢复后 `HARSH_THEN_FAULT` verify 绿

## Task Commits

1. **Task 1: gen --rate/--duration + loadtest → baseline.md** - `b5415fd` (feat)
2. **Task 2: drill_watermark_stall 停滞→恢复断言** - `553ecce` (feat)

**Plan metadata:** （本 SUMMARY；按编排指示不更新 STATE.md / ROADMAP.md）

## Files Created/Modified

- `projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py` — 速率/冻结/partial 模式
- `projects/p03-vehicle-monitoring/scripts/loadtest.sh` — 热身丢弃 + Prom/CH 刮取 → baseline
- `projects/p03-vehicle-monitoring/docs/baseline.md` — OrbStack arm64 实测表
- `projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh` — 冻结心跳 stall + verify recover
- `projects/p03-vehicle-monitoring/Makefile` — `loadtest` / `drill-watermark`
- `projects/p03-vehicle-monitoring/README.md` — §5.4 压测与演练入口

## Decisions Made

- 压测参数采用 RESEARCH 建议起点：100 eps × 120s，热身 45s 声明丢弃
- 因 `assignTimestampsAndWatermarks` 在 KafkaSource(noWatermarks) 下游，Source `currentEmitEventTimeLag` 在冻结期间可近 0；演练改为断言 CEP `currentInputWatermark` 停滞 + 墙钟推算 lag 上升（仍保留 Source lag 诊断日志；UI Watermarks 为 human-check 副证）
- 冻结 HEARTBEAT 使用 `vin-count=8` 覆盖多分区，避免 idleness 掩盖停滞

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Source emit-lag 不能单独证明 WM 停滞**
- **Found during:** Task 2
- **Issue:** 冻结 HEARTBEAT ≥45s 后 `currentEmitEventTimeLag{Source:_kafka_vehicle_events}` 仍约 10ms，导致初版断言失败
- **Fix:** 改为读取 `flink_taskmanager_job_task_currentInputWatermark{task_name=cep_harsh_then_fault}`，用 `wall_clock - wm` 推算 lag，并断言 `wm_delta≈0`
- **Files modified:** `scripts/drill_watermark_stall.sh`, `README.md`
- **Verification:** OrbStack `make drill-watermark` → wm_delta=0、lag_growth≈53549ms、recover verify 绿
- **Committed in:** `553ecce`

**2. [Rule 3 - Blocking] MinIO/TM 宕机与 Kafka backlog 干扰演练**
- **Found during:** Task 2
- **Issue:** 端口 9000 被其他项目 `saa-minio` 占用导致本仓库 MinIO 退出、TM 失败；大流量 backlog 会推进 watermark 掩盖冻结演示
- **Fix:** 临时停用冲突容器并重启 `fll-minio` + TM；consumer group reset `--to-latest` 后重提作业；README 增加 backlog 提示
- **Files modified:** `README.md`（文档）；运行时 compose/作业（不入库）
- **Verification:** 作业 RUNNING 后 drill exit 0
- **Committed in:** `553ecce`（文档部分）

**Total deviations:** 2 auto-fixed (Rule 1 ×1, Rule 3 ×1)
**Impact on plan:** 未改变交付物；增强了断言可靠性与运维提示。无范围蔓延。

## Issues Encountered

- 负载压测后 CH `vehicle_window_metrics` 增量曾为 0（当时作业消费/checkpoint 受 MinIO 影响）；baseline 主数字以 Prometheus produce/lag/ckpt/restarts 为准，属诚实实测。
- recover 首轮 verify 偶发 match=0（随后重试绿），脚本已有轮询窗口。

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- VEH-06 两条演练入口可执行；VEH-07（ADR/简历/架构页）可接着 03-03
- 无 k6；无编造 SLA

## Self-Check: PASSED

- FOUND: `projects/p03-vehicle-monitoring/docs/baseline.md`
- FOUND: `projects/p03-vehicle-monitoring/scripts/loadtest.sh`
- FOUND: `projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh`
- FOUND: commit `b5415fd`
- FOUND: commit `553ecce`

---
*Phase: 03-p03*
*Completed: 2026-07-18*
