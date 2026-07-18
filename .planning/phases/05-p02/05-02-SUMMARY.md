---
phase: 05-p02
plan: 02
subsystem: streaming
tags: [flink, jedis, redis, postgres, clickhouse, kafka, rule-scorer, topk, checkpointed]

requires:
  - phase: 05-p02/01
    provides: ParseBehaviorJson GREEN / RealtimeRecoJob 透传骨架 / Makefile gen/submit / BehaviorEvent POJO
provides:
  - SessionFeatureFunction MapState 双通道特征（Keyed State）
  - RedisFeatureWriter jedis Pipeline + CheckpointedFunction（at-least-once）
  - CatalogLoader PG reco_items + RuleScorer/TopKScoreFunction（REDIS|STATE_ONLY）
  - ClickHouseRecoSink + Kafka reco.results 双写
  - make match / verify.sh CH 权威绿路径（OrbStack 实测）
affects: [05-03]

tech-stack:
  added: []
  patterns:
    - 双通道：Keyed State 随流 FeatureSnapshot + Redis feature:{userId}:* 攒批写
    - 打分读 Redis 优先，失败 STATE_ONLY，作业不 FAIL
    - verify 仅认 flinklab.reco_results count；Kafka/Redis 仅 diag
    - CatalogLoader Class.forName + TopK ensureCatalog 懒加载抗 Shade/竞态

key-files:
  created:
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/model/FeatureSnapshot.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/feature/SessionFeatureFunction.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/feature/RedisFeatureWriter.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/catalog/CatalogLoader.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/score/RuleScorer.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/score/TopKScoreFunction.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/score/ScoredItem.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/model/CatalogItem.java
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/sink/ClickHouseRecoSink.java
    - projects/p02-realtime-reco/src/test/java/com/flywhl/flinklab/p02/feature/SessionFeatureFunctionTest.java
  modified:
    - projects/p02-realtime-reco/src/main/java/com/flywhl/flinklab/p02/RealtimeRecoJob.java
    - projects/p02-realtime-reco/scripts/verify.sh
    - projects/p02-realtime-reco/Makefile
    - projects/p02-realtime-reco/pom.xml
    - docker/docker-compose.yml
    - docker/.env

key-decisions:
  - "Redis 写语义文档锁定 at-least-once；写失败 catch+metric，主流 FeatureSnapshot 继续"
  - "TopKScoreFunction feature_source=REDIS|STATE_ONLY；读失败不抛死作业"
  - "CatalogLoader 显式 Class.forName(org.postgresql.Driver) + TopK ensureCatalog 懒重试"
  - "compose PG_HOST_PORT/REDIS_HOST_PORT 可覆盖；本机 .env 用 15432/16379 避让端口冲突"

patterns-established:
  - "p02 作业图：Parse→keyBy→SessionFeature→RedisWriter→TopK→Kafka+CH；uid 前缀 p02-"
  - "make match = truncate→cancel→submit→gen→轮询 verify（CH 权威）"

requirements-completed: [RECO-02]

duration: 13min
completed: 2026-07-18
---

# Phase 05 Plan 02: RECO-02 双通道特征 + Top-K 闭环 Summary

**双通道在线特征（Keyed State + Redis Checkpointed Pipeline）+ PG catalog 规则 Top-K + Kafka/CH 双写，OrbStack make match 以 ClickHouse reco_results 放行**

## Performance

- **Duration:** 13 min
- **Started:** 2026-07-18T04:08:29Z
- **Completed:** 2026-07-18T04:21:35Z
- **Tasks:** 3/3
- **Files modified:** 16

## Accomplishments

- `SessionFeatureFunction` MapState 累积 item/category 亲和与 clickCount，输出 `FeatureSnapshot`；`RedisFeatureWriter` 按 e07-C7 模式 at-least-once 写 `feature:{userId}:*`
- `RuleScorerTest` / `SessionFeatureFunctionTest` 全绿；`TopKScoreFunction` 支持 REDIS→STATE_ONLY 降级
- `make match` OrbStack 实测：`reco_results_match=80`，exit 0

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: SessionFeatureFunctionTest** - `0c21c74` (test)
2. **Task 1 GREEN: SessionFeature + RedisFeatureWriter** - `7480f18` (feat)
3. **Task 2: CatalogLoader + RuleScorer Top-K GREEN** - `54ee8f9` (feat)
4. **Task 3: 双写接线 + make match 绿** - `7119133` (feat)

**Plan metadata:** （docs close-out 提交见下）

## Files Created/Modified

- `FeatureSnapshot` / `SessionFeatureFunction` / `RedisFeatureWriter` — 双通道特征
- `CatalogLoader` / `CatalogItem` / `RuleScorer` / `ScoredItem` / `TopKScoreFunction` — 目录与打分
- `ClickHouseRecoSink` / `RealtimeRecoJob` — CH/Kafka 双写整图
- `verify.sh` / `Makefile match` — CH 权威验收
- `pom.xml` — 移除 RuleScorerTest exclude
- `docker/docker-compose.yml` + `.env` — 宿主机端口可覆盖

## Decisions Made

- Redis 写不假装 exactly-once；checkpoint 刷尾巴 + Operator ListState
- 打分热路径：Redis 点查成功标 REDIS，否则用随流 State 标 STATE_ONLY
- Wave 0 `CatalogItem`/`ScoredItem`/`RuleScorer.topK` 契约保留；`ItemCatalog` 经 CatalogLoader 桥接
- 本机 5432/6379 被其他栈占用时，`.env` 使用 `PG_HOST_PORT=15432` / `REDIS_HOST_PORT=16379`（容器内主机名不变）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] 本机 5432/6379 被其他容器占用导致 fll-postgres/redis 无法加入 compose 网络**
- **Found during:** Task 3（make match）
- **Issue:** `saa-postgres`/`saa-redis` 占用宿主机端口；Flink TM 解析到错误端点，PG/Redis 协议连接失败
- **Fix:** compose 支持 `PG_HOST_PORT`/`REDIS_HOST_PORT`；`.env` 改为 15432/16379；容器加入 `flink-learning-lab_default`
- **Files modified:** `docker/docker-compose.yml`, `docker/.env`
- **Verification:** 容器网络内 `psql`/`PING` 成功；`make match` 绿
- **Committed in:** `7119133`

**2. [Rule 1 - Bug] Shade 后 TopK open 偶发 `No suitable driver` 导致 catalog 空、无落库**
- **Found during:** Task 3（make match 首轮失败）
- **Issue:** TopK `open()` 时 JDBC 驱动未对当前类加载器就绪，catalog 空则永不 emit
- **Fix:** `CatalogLoader` 显式 `Class.forName("org.postgresql.Driver")` + Properties 超时；`TopKScoreFunction.ensureCatalog()` 在 flatMap 懒重试
- **Files modified:** `CatalogLoader.java`, `TopKScoreFunction.java`
- **Verification:** `make match` → `reco_results_match=80`
- **Committed in:** `7119133`

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** 无 scope 蔓延；压测/drill/文档仍属 05-03。

## Issues Encountered

None beyond the documented deviations.

## User Setup Required

None - no external service configuration required.若本机 5432/6379 已被占用，保持 `docker/.env` 中 `PG_HOST_PORT`/`REDIS_HOST_PORT` 覆盖即可（Flink 作业仍连容器主机名 `postgres`/`redis`）。

## Next Phase Readiness

- RECO-02 闭环可观察；05-03 可接 loadtest baseline、`drill_redis_degrade --implemented`、ADR/ARCHITECTURE/RESUME
- `make match` 依赖 default 基座 + 可达的 fll-postgres/fll-redis（同 compose 网络）

## Self-Check: PASSED

- 产物文件存在：`SessionFeatureFunction` / `RedisFeatureWriter` / `RuleScorer` / `CatalogLoader` / `ClickHouseRecoSink` / `verify.sh`
- 任务提交 `0c21c74` / `7480f18` / `54ee8f9` / `7119133` 均在 git log

---
*Phase: 05-p02*
*Completed: 2026-07-18*
