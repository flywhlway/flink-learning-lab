---
phase: 03-p03
plan: 01
subsystem: observability
tags: [flink, window, clickhouse, grafana, veh-05, dual-datasource]

requires:
  - phase: 03-p03-00
    provides: EventCountAggTest RED、verify_dashboard 骨架、vehicle_window_metrics DDL、Grafana CH DS / dashboards provider
provides:
  - VehicleWindowMetricsJob 旁路窗口作业（30s tumbling → CH）
  - EventCountAgg GREEN + ClickHouseWindowMetricsSink
  - p03-vehicle-overview.json 双 DS 大盘 + verify_dashboard GREEN
  - ANOMALY-THRESHOLDS.md 演示默认阈值条文
affects: [03-02, VEH-05, VEH-06]

tech-stack:
  added: []
  patterns: [旁路窗口作业 D-02、Grafana 双 DS provisioning、异常=阈值面板 D-04]

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleWindowMetricsJob.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/window/EventCountAgg.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/window/AttachWindowMeta.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/window/WindowMetricsRow.java
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseWindowMetricsSink.java
    - projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json
    - projects/p03-vehicle-monitoring/docs/ANOMALY-THRESHOLDS.md
  modified:
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/JobConfig.java
    - projects/p03-vehicle-monitoring/scripts/verify_dashboard.sh
    - projects/p03-vehicle-monitoring/Makefile
    - docker/Makefile
    - projects/p03-vehicle-monitoring/README.md

key-decisions:
  - "窗口默认 30s tumbling（非 1m），README 声明以便 OrbStack 快速落库"
  - "JobConfig.from(args, defaultJobName, defaultGroupId) 重载，告警作业默认不变"
  - "异常阈值演示默认：近 5m MATCH≥5、max(harsh_count)≥3，标非生产 SLA"

patterns-established:
  - "Pattern: 旁路作业 uid 前缀 p03-wm- + 独立 groupId p03-window-metrics"
  - "Pattern: verify_dashboard 检查插件类型 + uid + search + CH metrics smoke"

requirements-completed: [VEH-05]

duration: 4min
completed: 2026-07-18
---

# Phase 3 Plan 01: VEH-05 窗口指标与 Grafana 大盘 Summary

**旁路 VehicleWindowMetricsJob 写入 CH vehicle_window_metrics，Grafana 双 DS 大盘可 provisioning 且 verify_dashboard 在 OrbStack 上 exit 0；VehicleAlertJob CEP/Gate 未改。**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-07-18T01:59:43Z
- **Completed:** 2026-07-18T02:04:01Z
- **Tasks:** 3/3
- **Files modified:** 12

## Accomplishments

- EventCountAggTest GREEN；`submit-window` 后 CH `vehicle_window_metrics` count≥1（实测 10）
- `p03-vehicle-overview.json` 顶层 dashboard document：CH 窗口/告警/阈值 + Prometheus 健康；Grafana search 命中
- `verify_dashboard.sh` OrbStack exit 0（插件 `grafana-clickhouse-datasource` + uid `p03-clickhouse` + CH smoke）
- ANOMALY-THRESHOLDS.md + README §5.3 大盘验证路径；阈值面板与文档数字一致

## Task Commits

1. **Task 1: 窗口作业 → CH vehicle_window_metrics** — `41c8012` (feat)
2. **Task 2: Grafana 双 DS 大盘 + verify_dashboard** — `18d330d` (feat)
3. **Task 3: 异常阈值条文 + README 大盘节** — `5793495` (docs)

**Plan metadata:** （本文件提交后回填）

## Files Created/Modified

- `VehicleWindowMetricsJob.java` — 旁路 main；WM 与告警一致；30s window；uid `p03-wm-*`
- `EventCountAgg.java` / `AttachWindowMeta.java` / `WindowMetricsRow.java` — 三分计数 + 窗口元信息
- `ClickHouseWindowMetricsSink.java` — HTTP SinkV2；`containsForbidden`；表名常量
- `JobConfig.java` — `from(args, defaultJobName, defaultGroupId)` 重载
- `p03-vehicle-overview.json` — 双 DS 面板（非 API wrapper）
- `verify_dashboard.sh` — datasources / search / CH smoke GREEN 路径
- `ANOMALY-THRESHOLDS.md` — ≥2 条演示默认阈值
- `Makefile` / `docker/Makefile` — `submit-window` / `submit-p03-window` / `verify-dashboard`
- `README.md` — 架构图 + §5.3 大盘验证 + 插件踩坑

## Decisions Made

- 窗口长度选 30s（RESEARCH A3 允许），避免 1m 窗口拖慢 OrbStack 验证
- Grafana 重启后插件与 provisioning 一次性就绪；无需改 `make up` profile
- 阈值数字明示「演示默认 / 非生产 SLA」，与面板 thresholds 对齐

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- 既有 Grafana 容器未带 Wave 0 的 `GF_INSTALL_PLUGINS` / CH DS：执行 `docker compose up -d --force-recreate grafana` 后插件与 search 立即健康（按计划 fallback 文档亦已写入 README）

## Known Stubs

None — Wave 0 的 EventCountAgg / dashboard JSON 缺口已由本 plan 填满。`loadtest.sh` / `drill_watermark_stall.sh` 完整逻辑仍属 03-02（非本 plan 范围）。

## OrbStack Verification

| Check | Result |
|-------|--------|
| `mvn -Dtest=EventCountAggTest test` | exit 0 |
| `make submit-window` + `make gen` | Job RUNNING；CH metrics count=10 |
| Grafana `/api/datasources` | 含 `grafana-clickhouse-datasource` + `p03-clickhouse` |
| Grafana `/api/search?query=p03` | 命中 `p03-vehicle-overview` |
| `bash scripts/verify_dashboard.sh` | exit 0 |
| `VehicleAlertJob` CEP/Gate | 本 plan 未修改该文件 |

## Threat Flags

无超出计划 `<threat_model>` 的新表面；Sink 校验与 dashboard 无密码已落实。

## TDD Gate Compliance

- RED 由 03-00 `69249a5` 建立
- GREEN：`41c8012` 交付 `EventCountAgg` 使 EventCountAggTest 通过

## Self-Check: PASSED

- FOUND: VehicleWindowMetricsJob.java, ClickHouseWindowMetricsSink.java, p03-vehicle-overview.json, ANOMALY-THRESHOLDS.md
- FOUND commits: 41c8012, 18d330d, 5793495
- VehicleAlertJob 未出现在本 plan 任务 diff
- 违禁词扫描：无 TODO/省略/请参考官网/自行实现
