---
phase: 05-p02
plan: 01
subsystem: streaming
tags: [flink, kafka, parse, makefile, gen, smoke, reco, junit]

requires:
  - phase: 05-p02/00
    provides: p02 独立 pom / ParseBehaviorJsonTest RED / p02-init / up-p02 / reco DDL
provides:
  - BehaviorEvent/RecoResult/ItemCatalog POJO 与 JobConfig 手写解析
  - ParseBehaviorJson GREEN（脏数据丢弃 + 字符集白名单）
  - RealtimeRecoJob Kafka→Parse 透传骨架（uid p02-*）
  - Makefile package/submit/gen + gen_reco_events.py feature-score
  - smoke_p02_profile.sh（profile 隔离 + up-p02 可达性）
affects: [05-02, 05-03]

tech-stack:
  added: []
  patterns:
    - Parse tryParse Optional.empty 丢弃；userId/itemId SAFE_ID + 拒引号反斜杠
    - JobConfig topK≤50；造数 MAX_RATE=5000（T-05-04）
    - 透传切片 print 占位；特征/打分/CH 双写留给 05-02
    - RuleScorerTest 暂 testExclude，待 05-02 交付后移除

key-files:
  created:
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/RealtimeRecoJob.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/JobConfig.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/ParseBehaviorJson.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/model/BehaviorEvent.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/model/RecoResult.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/model/ItemCatalog.java
    - projects/p02-realtime-reco/Makefile
    - projects/p02-realtime-reco/scripts/gen_reco_events.py
    - scripts/smoke_p02_profile.sh
  modified:
    - projects/p02-realtime-reco/pom.xml

key-decisions:
  - "BehaviorEvent 字段名对齐 Wave 0 单测 eventTime（非 plan 文案 eventTimeMs）"
  - "RealtimeRecoJob 05-01 仅 print 透传，不接 CH/Kafka results"
  - "pom testExclude RuleScorerTest 直至 05-02，避免挡住 Parse GREEN"

patterns-established:
  - "p02 算子 uid 前缀 p02-；submit 固定 -c RealtimeRecoJob + jar 通配"
  - "smoke：config 隔离必过；基座未 up 则 FAIL 提示 make up（不假绿）"

requirements-completed: [RECO-01]

duration: 3min
completed: 2026-07-18
---

# Phase 05 Plan 01: RECO-01 启动面垂直切片 Summary

**ParseBehaviorJson GREEN + RealtimeRecoJob Kafka 透传 + gen feature-score + smoke_p02_profile 验证独立 profile**

## Performance

- **Duration:** 3 min
- **Started:** 2026-07-18T04:04:14Z
- **Completed:** 2026-07-18T04:06:44Z
- **Tasks:** 3/3
- **Files modified:** 10

## Accomplishments

- `ParseBehaviorJsonTest` 由 Wave 0 RED 转 GREEN：合法 VIEW|CLICK|CART|BUY 保留，缺字段/脏字符/非法 eventType 丢弃
- `RealtimeRecoJob` 可 shade 打包；`make submit` 固定主类；`gen_reco_events.py --scenario feature-score` 写 `reco.events`
- `smoke_p02_profile.sh` 绿：default 无 `p02-init`、`--profile p02` 有之；OrbStack 上 `up-p02` 后 topics + `reco_items=50`

## Task Commits

Each task was committed atomically:

1. **Task 1: 模型 + JobConfig + ParseBehaviorJson GREEN** - `8e291f0` (feat)
2. **Task 2: RealtimeRecoJob 透传 + Makefile + gen_reco_events.py** - `d7ecb4b` (feat)
3. **Task 3: profile 冒烟 — smoke_p02_profile + up-p02 可达性检查** - `c467a05` (feat)

**Plan metadata:** `f0be27e` (docs: complete RECO-01 vertical slice plan)

## Files Created/Modified

- `BehaviorEvent.java` / `RecoResult.java` / `ItemCatalog.java` — 行为/结果/目录 POJO
- `JobConfig.java` — 手写 `--key`；默认 `reco.events`/`reco.results`/`top-k=5`/redis/pg
- `ParseBehaviorJson.java` — FlatMap + `tryParse`；T-05-01 字符集白名单
- `RealtimeRecoJob.java` — KafkaSource → parse → print（uid `p02-*`）
- `Makefile` — package/submit/gen/verify/match/loadtest/drill-redis
- `scripts/gen_reco_events.py` — feature-score + rate 模式（MAX_RATE 封顶）
- `scripts/smoke_p02_profile.sh` — profile 隔离 + 运行时 up-p02 断言
- `pom.xml` — 暂排除 `RuleScorerTest` 编译

## Decisions Made

- 单测契约优先：`eventTime` 访问器对齐 Wave 0，不改测试去迁就 plan 文案 `eventTimeMs`
- 透传切片用 `print()` 占位，避免过早接 CH 双写膨胀 scope
- `RuleScorerTest` 用 compiler/surefire exclude 隔离，05-02 交付 `RuleScorer`/`CatalogItem` 后移除

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] 排除 RuleScorerTest 编译以免挡住 Parse GREEN**
- **Found during:** Task 1
- **Issue:** Wave 0 `RuleScorerTest` 引用尚未交付的 `CatalogItem`/`FeatureSnapshot`/`RuleScorer`，`mvn -Dtest=ParseBehaviorJsonTest` 仍会 testCompile 失败
- **Fix:** `pom.xml` 对 `**/score/RuleScorerTest.java` 设置 `testExcludes` / surefire `excludes`，并注释待 05-02 移除
- **Files modified:** `projects/p02-realtime-reco/pom.xml`
- **Verification:** `mvn -q -Dtest=ParseBehaviorJsonTest test` 退出 0
- **Committed in:** `8e291f0`

**2. [Rule 2 - Correctness] smoke 基座未 up 时 FAIL 而非跳过假绿**
- **Found during:** Task 3
- **Issue:** 计划要求运行时检查且「基座未 up 不假绿」；本机曾仅有 kafka/CH 而 redis/postgres Created
- **Fix:** smoke 在 config 通过后检测 kafka+postgres running；否则 FAIL 并提示 `make up`；可达则 `make up-p02` + topics + count≥50
- **Files modified:** `scripts/smoke_p02_profile.sh`
- **Verification:** 拉起 redis/postgres 后 `bash scripts/smoke_p02_profile.sh` 退出 0
- **Committed in:** `c467a05`

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 correctness)
**Impact on plan:** 无 scope 蔓延；特征/打分/verify 绿仍属 05-02。

## Issues Encountered

None beyond the documented deviations.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- 05-02 可接 SessionFeature / RedisWriter / RuleScorer / CH+Kafka 双写并移除 RuleScorerTest exclude
- `make match` / `verify.sh` 仍依赖 CH 有推荐行（本切片透传无落库）
- 压测 baseline 与 drill-redis `--implemented` 留给 05-03

## Self-Check: PASSED

- 产物文件 9/9 存在（含 pom 修改）
- 任务提交 `8e291f0` / `d7ecb4b` / `c467a05` 均在 git log

---
*Phase: 05-p02*
*Completed: 2026-07-18*
