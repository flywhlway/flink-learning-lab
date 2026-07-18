---
phase: 03-p03
plan: 00
subsystem: observability
tags: [grafana, clickhouse, nyquist, wave0, loadtest, watermark, junit]

requires:
  - phase: 02-p03-broadcast
    provides: CEP 告警链路、verify.sh CH 权威、p03-init 双 POST DDL 模式
provides:
  - EventCountAggTest RED 夹具（create/add/merge 契约）
  - verify_dashboard.sh / loadtest.sh / drill_watermark_stall.sh 失败态骨架
  - vehicle_window_metrics DDL + Grafana ClickHouse DS / dashboards provider 钩子
  - 根 README SSOT 登记 grafana-clickhouse-datasource
affects: [03-01, 03-02, VEH-05, VEH-06]

tech-stack:
  added: [grafana-clickhouse-datasource via GF_INSTALL_PLUGINS]
  patterns: [Wave0 RED 门禁骨架、p03-init 分 POST DDL、Grafana file provider 挂载项目 dashboards]

key-files:
  created:
    - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/window/EventCountAggTest.java
    - projects/p03-vehicle-monitoring/scripts/verify_dashboard.sh
    - projects/p03-vehicle-monitoring/scripts/loadtest.sh
    - projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh
    - projects/p03-vehicle-monitoring/sql/clickhouse_window_metrics.sql
    - projects/p03-vehicle-monitoring/monitoring/dashboards/.gitkeep
    - docker/config/grafana/provisioning/datasources/clickhouse.yml
    - docker/config/grafana/provisioning/dashboards/dashboards.yml
  modified:
    - docker/docker-compose.yml
    - README.md

key-decisions:
  - "EventCountAgg API 锁定为 AggregateFunction + 嵌套 Counts{eventCount,harshCount,dtcCount}，供 03-01 GREEN"
  - "Grafana 大盘 JSON 落仓 projects/.../monitoring/dashboards，compose 挂载不绑 --profile p03"
  - "loadtest/drill Wave0 用 --implemented 门闩默认 exit 1，禁止伪 GREEN"

patterns-established:
  - "Pattern: Wave0 脚本 set -euo pipefail + FAIL 非 0，与 verify.sh 纪律一致"
  - "Pattern: p03-init 每 SQL 文件一次 wget --post-file（CH HTTP 禁多语句）"
  - "Pattern: 双数据源 provisioning — Prometheus 既有 + ClickHouse 新 YAML"

requirements-completed: [VEH-05, VEH-06, VEH-07]

duration: 2min
completed: 2026-07-18
---

# Phase 3 Plan 00: Wave 0 Nyquist 夹具 Summary

**落地 VEH-05/06 失败态门禁与窗口 DDL/Grafana provisioning 骨架，建立 EventCountAgg RED→GREEN 与大盘/压测/演练反馈环，未实现窗口作业与完整面板。**

## Performance

- **Duration:** ~2 min
- **Started:** 2026-07-18T01:57:02Z
- **Completed:** 2026-07-18T01:58:40Z
- **Tasks:** 3/3
- **Files modified:** 10

## Accomplishments

- EventCountAggTest 可被 surefire 发现且当前编译失败（RED）；契约锁定 HEARTBEAT/HARSH_ACCEL/DTC 与 merge 相加
- `verify_dashboard.sh` / `loadtest.sh` / `drill_watermark_stall.sh` 均可 `bash -n`，直接执行均非 0
- `vehicle_window_metrics` DDL + ClickHouse DS（uid `p03-clickhouse`）+ dashboards provider + `GF_INSTALL_PLUGINS` + 根 README SSOT；`docker compose config -q` 通过

## Task Commits

1. **Task 1: RED 夹具 — EventCountAggTest + verify_dashboard.sh** — `69249a5` (test)
2. **Task 2: RED 骨架 — loadtest.sh + drill_watermark_stall.sh** — `a56bc90` (test)
3. **Task 3: 骨架 — window DDL + Grafana provisioning + SSOT** — `cfe6d69` (feat)

**Plan metadata:** `d0e608f` (docs: complete plan)

## Files Created/Modified

- `EventCountAggTest.java` — 窗口累加器 RED 契约（create/add/merge）
- `scripts/verify_dashboard.sh` — JSON 非空 + Grafana `/api/datasources` 与 `/api/search?query=p03`
- `scripts/loadtest.sh` — 压测入口骨架，声明将写 `docs/baseline.md`
- `scripts/drill_watermark_stall.sh` — frozen-eventTime stall→recover 演练入口骨架
- `sql/clickhouse_window_metrics.sql` — `flinklab.vehicle_window_metrics` DDL
- `monitoring/dashboards/.gitkeep` — 大盘目录占位（无伪造业务面板 JSON）
- `docker/.../clickhouse.yml` / `dashboards.yml` — Grafana provisioning
- `docker/docker-compose.yml` — grafana 插件/挂载；p03-init 第三 POST
- `README.md` — 版本矩阵登记 `grafana-clickhouse-datasource`

## Decisions Made

- 累加器类型命名为 `EventCountAgg.Counts`，字段 `eventCount`/`harshCount`/`dtcCount`（与 RESEARCH / 03-01 对齐）
- ClickHouse DS 使用 lab 密码 `flinklab123`（T-03-01；文档注明勿用于生产）
- default `make up` 不加 `--profile p03`；Grafana 挂载空 dashboards 目录可接受

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

| Stub | File | Reason |
|------|------|--------|
| EventCountAgg 生产类缺失 | `EventCountAggTest.java` 引用 | Wave 0 故意 RED；03-01 GREEN |
| 无 `p03-vehicle-overview.json` | `verify_dashboard.sh` | 故意缺文件 → exit 1；03-01 交付面板 |
| loadtest/drill 完整逻辑 | `loadtest.sh` / `drill_watermark_stall.sh` | Wave 0 `--implemented` 门闩；03-02 接线 |

## OrbStack / Docker notes

- 本机 `docker compose config -q` **成功**（Docker Compose v5.1.2 可用）
- Wave 0 **未**要求拉起 Grafana 实测插件下载或跑通 loadtest/drill；脚本失败态为契约正确
- 未伪造任何 baseline 实测数字或完整大盘 JSON

## Threat Flags

无新增威胁面超出计划 `<threat_model>`：`clickhouse.yml` secureJsonData 与 `GF_INSTALL_PLUGINS` 已在 T-03-01 / T-03-SC 登记。

## TDD Gate Compliance

- RED gate commit 存在：`69249a5`（`test(03-00): ...`）
- GREEN 故意推迟至 03-01（本 Wave 成功标准要求 EventCountAggTest ≠0）— 非违规

## Self-Check: PASSED

- 全部关键产物文件 FOUND
- 提交 `69249a5` / `a56bc90` / `cfe6d69` FOUND
- VehicleAlertJob 未出现在本 plan diff
- 违禁词扫描：无 TODO/省略/请参考官网 等
