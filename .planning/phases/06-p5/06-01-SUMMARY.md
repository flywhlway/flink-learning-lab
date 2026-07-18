---
phase: 06-p5
plan: 01
subsystem: infra
tags: [benchmark, prometheus, flink, orbstack, prod-01, compose]

# Dependency graph
requires:
  - phase: 06-p5
    provides: Wave 0 RED harness（run_matrix / collect_metrics / Makefile）与 docs 13 骨架
provides:
  - 可运行裁剪矩阵 harness（make matrix / dry-run）
  - 仓库级 benchmark/baseline.md（OrbStack arm64 实测九单元格）
  - docs 模块 13 完成态（权威路径 benchmark/）
affects: [06-02, 06-04, PROD-01, docs/13-performance]

# Tech tracking
tech-stack:
  added: []
  patterns: [compose Flink 压测非 K8s, Python gen + Labs.events --eps, Prom 值班五指标 key=value, SKIPPED+原因禁假数]

key-files:
  created:
    - benchmark/baseline.md
  modified:
    - benchmark/scripts/run_matrix.sh
    - benchmark/scripts/collect_metrics.py
    - benchmark/Makefile
    - benchmark/README.md
    - docs/13-performance/README.md
    - docs/README.md
    - examples/e01-hello-flink/src/main/java/com/flywhl/flinklab/e01/KafkaClickstreamWindowJob.java
    - examples/e10-cep/src/main/java/com/flywhl/flinklab/e10/C5VehicleDtcPatternJob.java

key-decisions:
  - "PROD-01 部署锁定 compose Flink；e10 负载用 Labs.events --eps（CEP 需 amount，与 gen_events 点击流不兼容）"
  - "20k / ForSt 本轮 SKIPPED+原因；热身默认 45s 并声明相对理想 3 分钟偏差"
  - "e01/e10 增补 --state-backend / --checkpoint-interval-ms / --unaligned 供矩阵旋钮"

patterns-established:
  - "矩阵单元格：cancel→submit→warmup discard→measure→Prom scrape→写 baseline 行"
  - "刮取失败重试 + 无过滤回退，禁止手填假数"

requirements-completed: [PROD-01]

# Metrics
duration: 25min
completed: 2026-07-18
---

# Phase 6 Plan 01: Benchmark 矩阵 → baseline.md Summary

**在 OrbStack arm64 / compose Flink 上跑通 D-01 裁剪压测矩阵，产出仓库级 `benchmark/baseline.md`（九单元格实测 + 20k/ForSt SKIPPED），并完成 docs 模块 13**

## Performance

- **Duration:** 25 min（含全量 matrix ≈18 min）
- **Started:** 2026-07-18T04:53:16Z
- **Completed:** 2026-07-18T05:18:38Z
- **Tasks:** 2/2
- **Files modified:** 9

## Accomplishments

- `make -C benchmark dry-run` 与 `make -C benchmark matrix` 均 exit 0；权威报告 `benchmark/baseline.md` 含 UTC 采集时间、Darwin arm64、OrbStack compose 环境、热身 45s 偏差声明
- 必跑覆盖：e01-J2 / e10 `C5VehicleDtcPatternJob` / p03 `VehicleAlertJob` × 1k&5k × HashMap&RocksDB 增量主路径 + 对齐 10s / 非对齐 30s 对照行
- docs 模块 13 升级为 ✅，链接 `benchmark/baseline.md`；项目级 baseline 明确不替代仓库级（D-03）

## Task Commits

Each task was committed atomically:

1. **Task 1: 实现裁剪矩阵 harness** - `d93bad0` (feat)
2. **Task 2: OrbStack 实测 baseline + 模块 13 完成态** - `365cb1c` (feat)

**Plan metadata:** （本 SUMMARY 提交；STATE/ROADMAP 按 orchestrator 指示不更新）

## Files Created/Modified

- `benchmark/scripts/run_matrix.sh` — D-01 九单元格编排 + baseline 写入 + Prom 重试
- `benchmark/scripts/collect_metrics.py` — 值班五指标刮取
- `benchmark/Makefile` — `matrix` / `dry-run` / `baseline`
- `benchmark/README.md` — 裁剪轴说明（去掉 e05/倾斜必跑诱惑）
- `benchmark/baseline.md` — 仓库级实测报告
- `docs/13-performance/README.md` / `docs/README.md` — 模块 13 完成态
- `KafkaClickstreamWindowJob.java` / `C5VehicleDtcPatternJob.java` — 矩阵旋钮参数

## Decisions Made

- e10 不走 Kafka `gen_events`：C5 模式依赖 `amount`，改用作业内 `Labs.events` RateLimiter（`--eps`）
- stretch 20k 因 `gen_vehicle_events` `MAX_RATE=5000` 与本机稳定性 SKIPPED；ForSt 非硬门禁 SKIPPED
- 接受 p03 高 `emitEventTimeLag` 为 earliest+历史 backlog 的实测观测，写入结论说明

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] e01/e10 缺少矩阵旋钮参数**
- **Found during:** Task 1
- **Issue:** e01 硬编码 30s checkpoint、无 state-backend；e10 C5 硬编码 eps=60 且无 backend/ckpt
- **Fix:** 为两作业增加 `--state-backend` / `--checkpoint-interval-ms` / `--unaligned`（e10 另加 `--eps`）
- **Files modified:** `KafkaClickstreamWindowJob.java`, `C5VehicleDtcPatternJob.java`
- **Committed in:** `d93bad0`

**2. [Rule 1 - Bug] 单元格 #8 Prom 刮取瞬时空结果**
- **Found during:** Task 2
- **Issue:** 对齐 10s 单元格指标全为 n/a
- **Fix:** 同配置补跑刮取写入实测值；harness 增加 scrape 重试与 20s 等待
- **Files modified:** `benchmark/baseline.md`, `benchmark/scripts/run_matrix.sh`
- **Committed in:** `365cb1c`

**Total deviations:** 2 auto-fixed (Rule 2 ×1, Rule 1 ×1)
**Impact on plan:** 必要正确性修复；无范围膨胀

## Issues Encountered

- p03 单元格事件时间 lag 极大（earliest 消费历史 backlog）——已在 baseline 结论如实记录
- e10 5k + RocksDB 单元格 ckpt≈9s、busy≈1000——保留实测，未改阈值

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- PROD-01 证据链齐全，可进入 06-02 Operator Blue/Green
- 无 K8s 依赖遗留；compose 基座命令已文档化

## Self-Check: PASSED

- FOUND: `benchmark/baseline.md`
- FOUND: `benchmark/scripts/run_matrix.sh`
- FOUND: `docs/13-performance/README.md`
- FOUND: commit `d93bad0`
- FOUND: commit `365cb1c`

---
*Phase: 06-p5*
*Completed: 2026-07-18*
