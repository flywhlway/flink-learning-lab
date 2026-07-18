---
phase: 05-p02
plan: 03
subsystem: streaming
tags: [flink, redis, degrade-drill, loadtest, baseline, adr, resume, documentation]

requires:
  - phase: 05-p02/02
    provides: 双通道特征 + Top-K 闭环 + make match CH 权威绿
provides:
  - drill_redis_degrade（stop fll-redis → CH feature_source=STATE_ONLY）
  - loadtest → docs/baseline.md OrbStack arm64 实测
  - ADR-0001 双通道 + ARCHITECTURE + RESUME + 八段式 README
  - docs/README 15-02 完成态；CHANGELOG/PHASES；qa_check 绿
affects: [phase-6-prod, milestone-close]

tech-stack:
  added: []
  patterns:
    - 恰好 2 条硬演练：drill-redis + loadtest（D-12）
    - baseline 吞吐用 gen 实际发送量/墙钟，非配置 eps 冒充实测
    - ADR 主题锁定双通道 Keyed State + Redis vs 纯状态/纯外存

key-files:
  created:
    - projects/p02-realtime-reco/scripts/drill_redis_degrade.sh
    - projects/p02-realtime-reco/scripts/loadtest.sh
    - projects/p02-realtime-reco/docs/baseline.md
    - projects/p02-realtime-reco/docs/adr/0001-dual-channel-features.md
    - projects/p02-realtime-reco/docs/ARCHITECTURE.md
    - projects/p02-realtime-reco/docs/RESUME.md
    - projects/p02-realtime-reco/README.md
  modified:
    - projects/p02-realtime-reco/Makefile
    - docs/README.md
    - CHANGELOG.md
    - PHASES.md

key-decisions:
  - "loadtest 墙钟吞吐用 gen 回报的实际 events 数，避免配置 100eps 冒充实测"
  - "drill EXIT trap 强制恢复 redis，避免污染后续 loadtest"
  - "教材链接指向 examples/e12-06-streaming-feature（非臆造目录名）"

patterns-established:
  - "p02 完成态三件套：make match + make drill-redis + make loadtest"
  - "FEATURE_SOURCE 白名单拼 SQL；CH 权威；Kafka/Redis 仅诊断"

requirements-completed: [RECO-03]

duration: 7min
completed: 2026-07-18
---

# Phase 05 Plan 03: RECO-03 压测/演练/文档包 Summary

**Redis 降级演练 + 项目级 loadtest/baseline + ADR/ARCHITECTURE/RESUME/八段式 README + 15-02 回填，p02 达单项目完成态**

## Performance

- **Duration:** 7 min
- **Started:** 2026-07-18T04:22:53Z
- **Completed:** 2026-07-18T04:29:37Z
- **Tasks:** 3/3
- **Files modified:** 11

## Accomplishments

- `make drill-redis` OrbStack 实测：stop `fll-redis` 后 CH `feature_source=STATE_ONLY` match=40，作业未长期 RESTARTING；EXIT trap 恢复 redis
- `make loadtest` 写入 `docs/baseline.md`：配置 100 eps×90s，计量段实际 7046 条 ≈ **78.29 eps**，lag 6ms，ckpt 91ms，restarts 0，CH +46910 行
- 文档包齐：ADR-0001 双通道、ARCHITECTURE、RESUME、八段式 README；`docs/README` 15-02 ✅；`qa_check.sh` PASS

## Task Commits

Each task was committed atomically:

1. **Task 1: Redis 降级演练 drill_redis_degrade 绿** - `d0427e8` (feat)
2. **Task 2: 项目级 loadtest 与 OrbStack baseline 实测** - `96bf56d` (feat)
3. **Task 3: p02 文档包与 15-02 完成态回填** - `cbe9206` (docs)

**Plan metadata:** `f673a4b` (docs: complete RECO-03 drills docs and baseline plan)

## Files Created/Modified

- `scripts/drill_redis_degrade.sh` — stop redis → STATE_ONLY CH 断言 → 恢复
- `scripts/loadtest.sh` — RATE 封顶、Prom/CH 刮数、写 baseline
- `docs/baseline.md` — OrbStack arm64 实测表
- `docs/adr/0001-dual-channel-features.md` — 双通道 ADR
- `docs/ARCHITECTURE.md` / `docs/RESUME.md` / `README.md` — 架构/简历/八段式
- `docs/README.md` 15-02 · `CHANGELOG.md` · `PHASES.md` — 编号与状态回填
- `Makefile` — drill-redis / loadtest 实现态入口

## Decisions Made

- baseline 吞吐主数字用 **实际发送量/墙钟**（78.29 eps），配置 100 eps 仅作参数记录；造数 sleep 开销导致低于配置（未下调 RATE）
- drill 在 Redis 停服期间检测 RESTARTING 并可 cancel+submit；本轮作业保持 RUNNING，无需重提
- Grafana 专用 JSON 不做（Discretion）；恰好 2 演练，无第三条 chaos 门禁

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] loadtest 曾用配置 RATE×DURATION 冒充墙钟吞吐**
- **Found during:** Task 2（首轮 baseline 写完后复核）
- **Issue:** gen 计量段实际只发 7046 条，脚本却把 produced_eps 算成 100.0
- **Fix:** 解析 `events=N`；baseline 改为 78.29 eps 实测；脚本同步
- **Files modified:** `scripts/loadtest.sh`, `docs/baseline.md`
- **Verification:** baseline 表含实测 78.29 eps / 7046 条
- **Committed in:** `96bf56d`

**2. [Rule 3 - Blocking] ADR/ARCHITECTURE 教材断链**
- **Found during:** Task 3（`qa_check.sh`）
- **Issue:** 链接写成不存在的 `e12-06-feature-dual-channel`
- **Fix:** 改为仓库实名 `examples/e12-06-streaming-feature`
- **Files modified:** ADR、ARCHITECTURE、README
- **Verification:** `bash scripts/qa_check.sh` → `== QA PASS ==`
- **Committed in:** `cbe9206`

## Threat Flags

无新增计划外威胁面；drill/loadtest/文档均在计划 threat_model（T-05-01/03/06）内。

## Known Stubs

None — Wave0 `--implemented` 门闩已移除；脚本与文档无 TODO/待填。

## Issues Encountered

- `qa_check` 中 examples 全仓 `mvn compile` 对 e07/e08 依赖解析 warn（aliyun 缓存 miss）——脚本将 mvn 失败记为 warn 不挡 PASS；与本 Phase 无关的既有环境问题。p02 独立模块此前已 `package`/`test` 绿。

## Next Phase Readiness

- RECO-03 交付完毕；Phase 5（p02）四 plan 均可宣称完成
- 下一里程碑焦点：P5 生产化（benchmark 全矩阵 / Operator / GitOps）

## Self-Check: PASSED

- FOUND: drill/loadtest/baseline/ADR/ARCHITECTURE/RESUME/README
- FOUND: commits d0427e8, 96bf56d, cbe9206
