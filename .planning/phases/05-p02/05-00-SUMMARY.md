---
phase: 05-p02
plan: 00
subsystem: infra
tags: [flink, jedis, clickhouse, postgres, kafka, junit, nyquist, wave0]

requires:
  - phase: 04-p01
    provides: 独立 pom / verify CH 权威 / --implemented 门闩 / compose profile 纪律
provides:
  - jedis 5.2.0 SSOT 矩阵行
  - p02-realtime-reco 最小独立 Maven 模块与 RED 单测夹具
  - verify/drill/loadtest 失败态骨架与 reco_results/reco_items DDL
  - docker p02-init profile 与 make up-p02（不污染 default up）
affects: [05-01, 05-02, 05-03]

tech-stack:
  added: [jedis 5.2.0（SSOT）, postgresql JDBC 42.7.4（p02 pom）]
  patterns:
    - Wave 0 RED：单测引用未交付符号，surefire/testCompile ≠ 0
    - 脚本 --implemented 门闩保持默认非 0（drill/loadtest）
    - compose 独立 --profile p02；PG 种子经 Makefile psql，不进 kafka 镜像

key-files:
  created:
    - projects/p02-realtime-reco/pom.xml
    - projects/p02-realtime-reco/src/test/java/com/flywhl/flinklab/p02/ParseBehaviorJsonTest.java
    - projects/p02-realtime-reco/src/test/java/com/flywhl/flinklab/p02/score/RuleScorerTest.java
    - projects/p02-realtime-reco/scripts/verify.sh
    - projects/p02-realtime-reco/scripts/drill_redis_degrade.sh
    - projects/p02-realtime-reco/scripts/loadtest.sh
    - projects/p02-realtime-reco/sql/clickhouse_reco_results.sql
    - projects/p02-realtime-reco/sql/postgres_reco_items.sql
  modified:
    - README.md
    - docker/docker-compose.yml
    - docker/Makefile

key-decisions:
  - "Wave 0 故意不实现 ParseBehaviorJson/RuleScorer/RealtimeRecoJob，testCompile 失败建立 RED 反馈环"
  - "drill/loadtest 用 --implemented 门闩；verify 走真实 CH 查询以便空表/未起时非 0"
  - "PG reco_items 50 行种子由 make up-p02 psql 注入，p02-init 仅 Kafka+CH"
  - "eventType 锁定 VIEW|CLICK|CART|BUY；Top-K=5；权重 VIEW=1/CLICK=3/CART=5/BUY=10"

patterns-established:
  - "p02 目录/脚本/compose 钩子复制 p01 纪律，不挂 examples 父工程"
  - "FEATURE_SOURCE 白名单 REDIS|STATE_ONLY 后再拼 SQL（T-05-01）"

requirements-completed: []  # Wave 0 仅 Nyquist 骨架；RECO-01–03 完整交付留给 05-01+

duration: 2min
completed: 2026-07-18
---

# Phase 05 Plan 00: Wave 0 Nyquist 骨架 Summary

**p02 独立模块 + jedis SSOT + Parse/RuleScorer RED 夹具 + verify/drill/loadtest 失败态骨架 + reco DDL/p02-init/up-p02**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-18T03:59:47Z
- **Completed:** 2026-07-18T04:02:00Z
- **Tasks:** 3/3
- **Files modified:** 11

## Accomplishments

- 根 README 版本矩阵登记 jedis **5.2.0**（D-02），p02 独立 pom 对齐 Flink 2.2.1 / Kafka connector / jackson / junit / jedis / postgresql
- `ParseBehaviorJsonTest` / `RuleScorerTest` 引用未交付生产类，`mvn -Dtest=…` 退出码 ≠ 0（RED）
- 三脚本可 `bash -n`，默认执行非 0；`flinklab.reco_results` + `reco_items`（50 种子）DDL 落仓
- `p02-init`（`--profile p02`）创建 `reco.events`/`reco.results` 并 POST CH DDL；`make up-p02` 额外 psql 种子；default `make up` 不加 p02

## Task Commits

Each task was committed atomically:

1. **Task 1: SSOT jedis + 最小 pom + Parse/RuleScorer RED 夹具** - `bc386e7` (test)
2. **Task 2: RED 骨架 — verify / drill-redis / loadtest + 双 DDL** - `0b2ae2d` (test)
3. **Task 3: 骨架 — p02-init + make up-p02** - `c32caec` (feat)

**Plan metadata:** （本 SUMMARY 提交后回填）

## Files Created/Modified

- `README.md` — jedis 5.2.0 SSOT 行
- `projects/p02-realtime-reco/pom.xml` — 独立模块脚手架
- `projects/p02-realtime-reco/src/test/java/.../ParseBehaviorJsonTest.java` — 行为 JSON 解析 RED
- `projects/p02-realtime-reco/src/test/java/.../score/RuleScorerTest.java` — 规则 Top-K RED
- `projects/p02-realtime-reco/scripts/verify.sh` — CH `reco_results` 权威出口
- `projects/p02-realtime-reco/scripts/drill_redis_degrade.sh` — Redis 降级演练门闩骨架
- `projects/p02-realtime-reco/scripts/loadtest.sh` — baseline 压测门闩骨架
- `projects/p02-realtime-reco/sql/clickhouse_reco_results.sql` — 单语句 CREATE
- `projects/p02-realtime-reco/sql/postgres_reco_items.sql` — CREATE + 50 INSERT
- `docker/docker-compose.yml` — `p02-init` service
- `docker/Makefile` — `up-p02` 目标

## Decisions Made

- Wave 0 不实现作业主类 / RedisWriter / CatalogLoader / baseline 实测（留给 05-01+）
- verify 采用 p01 式真实 CH 查询（空表/未起 → 非 0），drill/loadtest 用 `--implemented` 门闩
- PG 种子放 Makefile（kafka 镜像无 psql），与 RESEARCH Discretion 一致
- 单测契约锁定 RESEARCH：eventType 大写四枚举、Top-K=5、BUY>VIEW、类目匹配加成

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] 撤回 Wave 0 对 RECO-01–03 的过早 mark-complete**
- **Found during:** close-out（state updates）
- **Issue:** PLAN frontmatter 列出 RECO-*，SDK mark-complete 会把整项标 Complete，但 Wave 0 仅 Nyquist 骨架，作业/绿路径/压测未交付
- **Fix:** REQUIREMENTS.md 勾选与 traceability 表改回 Pending；SUMMARY `requirements-completed` 置空
- **Files modified:** `.planning/REQUIREMENTS.md`, `05-00-SUMMARY.md`
- **Committed in:** docs close-out commit

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** 防止里程碑审计误判 RECO 已完成；无生产代码范围蔓延。

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Nyquist RED 夹具齐备，05-01 可交付解析/模型/作业骨架并转 GREEN
- `make up-p02` 依赖 default 基座中 redis/postgres 已可达
- 未实现 `RealtimeRecoJob`、完整 verify 绿路径、双通道特征、loadtest baseline

## Self-Check: PASSED

- 产物文件 11/11 存在
- 任务提交 `bc386e7` / `0b2ae2d` / `c32caec` 均在 git log

---
*Phase: 05-p02*
*Completed: 2026-07-18*
