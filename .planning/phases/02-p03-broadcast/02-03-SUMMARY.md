---
phase: 02-p03-broadcast
plan: 03
subsystem: testing
tags: [flink-cep, broadcast, verify-switch, publish-control, pattern_id, VEH-03, VEH-04, orbstack-e2e]

requires:
  - phase: 02-p03-broadcast/02-02b
    provides: 三 CEP + PatternActivationGate + pattern_id 落库 + control topic
provides:
  - gen 三 scenario + --publish-control（D-12）
  - Makefile verify-switch（TRUNCATE→control→gen→PATTERN_ID CH 断言）
  - README ↔ PATTERN-LIBRARY 交叉引用与切换剧本（D-03/D-10）
  - OrbStack 默认 HARSH_THEN_FAULT 回归 + TRIPLE_HARSH 切换 e2e 绿
affects: [phase-3-p03-ops, VEH-06, VEH-07]

tech-stack:
  added: []
  patterns:
    - CH-authoritative PATTERN_ID verify with whitelist
    - publish-control JSON + version monotonic Broadcast gate
    - p03-init CREATE/ALTER split HTTP POST（CH 禁多语句）

key-files:
  created:
    - projects/p03-vehicle-monitoring/sql/clickhouse_alerts_alter.sql
  modified:
    - projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py
    - projects/p03-vehicle-monitoring/Makefile
    - projects/p03-vehicle-monitoring/README.md
    - projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql
    - docker/docker-compose.yml
    - docs/README.md
    - CHANGELOG.md
    - PHASES.md

key-decisions:
  - "切换验收主路径为 make verify-switch（CH pattern_id），禁止 Kafka 单独放行"
  - "p03-init DDL 拆成 CREATE + ALTER 两次 POST（CH 24.8 HTTP 无 multiquery）"
  - "--auto 链下 Task 3 human-verify 以 OrbStack CH 断言输出为自动化验收证据"

patterns-established:
  - "Pattern: verify-switch = TRUNCATE → publish-control → scenario gen → PATTERN_ID verify poll"
  - "Pattern: match 别名 → match-harsh-fault；默认未发 control 仍绿（D-06）"

requirements-completed: [VEH-03, VEH-04]

duration: 5min
completed: 2026-07-18
---

# Phase 02 Plan 03: 造数切换剧本 + e2e + README Summary

**造数三 scenario + `--publish-control`、`make verify-switch` 在 OrbStack 上跑通默认 HARSH_THEN_FAULT 回归与 TRIPLE_HARSH 切换，README 交叉引用 PATTERN-LIBRARY，qa_check 绿**

## Performance

- **Duration:** 5 min
- **Started:** 2026-07-18T01:13:32Z
- **Completed:** 2026-07-18T01:18:49Z
- **Tasks:** 3/3（Task 3 在 `--auto` 链下以自动化 CH 证据验收）
- **Files modified:** 9

## Accomplishments

- 扩展 `gen_vehicle_events.py`：`match`→`match-harsh-fault` 别名、`match-triple-harsh`（三次 HARSH 间无 HEARTBEAT）、`match-dtc-pair`、`--publish-control`（JSON/文件 → `vehicle.pattern.control`，acks=all）
- Makefile：`truncate-alerts` + `verify-switch`（CONTROL_VERSION 可递增轮询 CH）
- OrbStack arm64 e2e：默认回归与 TRIPLE_HARSH 切换均 exit 0；CH 为唯一权威
- README 八段式更新 + PATTERN-LIBRARY 链接；CHANGELOG/PHASES/docs 15-03；`qa_check.sh` → `== QA PASS ==`
- 修复 `p03-init` DDL 多语句 400（拆 ALTER 文件）

## Task Commits

1. **Task 1: 造数 scenarios + publish-control + verify-switch** - `1618c5f` (feat)
2. **Task 2: OrbStack 切换 e2e + README/登记/qa_check** - `bb18529` (feat)
3. **Task 3: 人工确认模式切换可观察** - 自动化验收（见下）；无单独代码 commit

**Plan metadata:** `5b3c963` (docs: complete plan；后续 STATE 决策补记另见)

## OrbStack e2e 证据（CH 权威断言）

环境：OrbStack `aarch64`；`make up-p03` exit 0；取消旧作业后 `make submit` → JobID `2b858ef14b266e50696ebadcffb00e6b` RUNNING。

### D-06 默认回归（未发 control）

```text
make truncate-alerts && make gen
→ ok alerts_match=1 pattern_id=HARSH_THEN_FAULT min_count=1 alerts_total=1
CH GROUP BY:
HARSH_THEN_FAULT	MATCH	1
```

### D-10 切换剧本（`make verify-switch CONTROL_VERSION=2`）

```text
TRUNCATE → publish-control {"activePatterns":["TRIPLE_HARSH"],"version":2}
→ gen match-triple-harsh
→ ok alerts_match=1 pattern_id=TRIPLE_HARSH min_count=1 alerts_total=1
verify-switch ok (PATTERN_ID=TRIPLE_HARSH)
CH GROUP BY:
TRIPLE_HARSH	MATCH	1
可选：PATTERN_ID=HARSH_THEN_FAULT → exit 1（match=0 total=1，符合未激活不落库）
```

### qa_check

```text
ok    docker compose config
ok    违禁词扫描
ok    Markdown 相对链接
== QA PASS ==
```

（`mvn compile` 对无关模块 e07/e08 有离线依赖告警；p03 `mvn test` 本计划内已绿。）

## Task 3 human-verify（`--auto` 链）

| 检查项 | 状态 | 证据 |
|--------|------|------|
| PATTERN-LIBRARY 三模式五元组齐全 | 自动化已确认 | `rg HARSH_THEN_FAULT\|TRIPLE_HARSH\|DTC_PAIR` 命中文档表 |
| 默认 match-harsh-fault → HARSH_THEN_FAULT verify | **自动化已绿** | 上文 CH 输出 |
| TRUNCATE→publish-control→triple-harsh→TRIPLE_HARSH verify | **自动化已绿** | `make verify-switch` + CH 输出 |
| 主路径非「重启作业换参数」；Kafka 不单独放行 | 自动化已确认 | README/verify.sh/Makefile 剧本；verify 失败时 Kafka 仅 diag |

**Auto-approved under `--auto` chain**（`workflow._auto_chain_active=true`）：以 ClickHouse `pattern_id` 断言为验收权威，未伪造 Grafana 截图。

**仍建议维护者目视（非阻塞）：** 打开 `docs/PATTERN-LIBRARY.md` 通读五元组表与「门控≠停 CEP 状态」段；Flink UI `http://localhost:8081` 确认作业 RUNNING。回复 `approved` 可记入正式人工签核；当前不阻塞 Phase 收尾。

## Files Created/Modified

- `scripts/gen_vehicle_events.py` — 三 scenario + publish-control
- `Makefile` — verify-switch / truncate-alerts
- `README.md` — 八段式 + PATTERN-LIBRARY + 切换剧本
- `sql/clickhouse_alerts.sql` + `clickhouse_alerts_alter.sql` — 拆分 DDL
- `docker/docker-compose.yml` — p03-init 两次 POST
- `docs/README.md` / `CHANGELOG.md` / `PHASES.md` — 登记与状态

## Decisions Made

- 切换验收锁定 `make verify-switch` + `PATTERN_ID` CH 断言（D-10）
- CH HTTP 多语句不可用 → CREATE/ALTER 分文件分 POST（Rule 3）
- Task 3 在 auto 链以 e2e CH 输出完成自动化验收，目视文档为非阻塞建议

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] p03-init ClickHouse DDL 多语句 400**
- **Found during:** Task 2（`make up-p03`）
- **Issue:** `clickhouse_alerts.sql` 含 CREATE+ALTER；CH 24.8 HTTP 禁多语句，且不认 `multiquery` 设置 → wget 400/404，表缺 `pattern_id`
- **Fix:** 拆出 `clickhouse_alerts_alter.sql`；compose 分两次 `--post-file`
- **Files modified:** `sql/clickhouse_alerts.sql`、`sql/clickhouse_alerts_alter.sql`、`docker/docker-compose.yml`
- **Verification:** `make up-p03` → p03-init Exited (0)；DESCRIBE 含 `pattern_id`
- **Committed in:** `bb18529`

**Total deviations:** 1 auto-fixed (Rule 3)
**Impact on plan:** 必要正确性修复；无范围蔓延

## Issues Encountered

- 集群上已有旧 p03 作业：先 `flink cancel` 再 submit 新 jar，否则 Broadcast 门控版本不会生效

## Known Stubs

None — 切换剧本与文档已接线；Grafana/压测/完整 ADR 明确属后续 Phase（非本计划 stub）。

## User Setup Required

None beyond 既有 OrbStack 基座（`docker make up && make init && make up-p03`）。本地 lab 账号沿用 `flinklab` / `flinklab123`，未新增真实密钥。

## Next Phase Readiness

- VEH-03/VEH-04 脚本与 e2e 路径可复现；后续 Phase 可接 Grafana 大盘 / 压测 / ADR（VEH-06/07）
- 复跑 `verify-switch` 时递增 `CONTROL_VERSION`

## Self-Check: PASSED

- FOUND: `projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py`
- FOUND: `projects/p03-vehicle-monitoring/Makefile`（verify-switch）
- FOUND: `projects/p03-vehicle-monitoring/README.md`（PATTERN-LIBRARY）
- FOUND: `projects/p03-vehicle-monitoring/docs/PATTERN-LIBRARY.md`
- FOUND: `1618c5f`、`bb18529` commits
- FOUND: `.planning/phases/02-p03-broadcast/02-03-SUMMARY.md`

---
*Phase: 02-p03-broadcast*
*Completed: 2026-07-18*
