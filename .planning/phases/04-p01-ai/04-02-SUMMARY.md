---
phase: 04-p01-ai
plan: 02
subsystem: streaming
tags: [log-ai, rule-tagger, feature-enricher, clickhouse, verify, kafka, flink]

requires:
  - phase: 04-p01-ai/01
    provides: LogEvent/LogResult、ParseLogJson、JobConfig、LogAiJob 骨架、p01-init/DDL
provides:
  - FeatureEnricher（Keyed State ERROR 计数 → featureJson）
  - RuleTagger（AUTH_FAIL/ERROR_BURST/NONE；单测 GREEN）
  - ClickHouseLogSink → flinklab.log_results
  - gen_log_events.py rule-auth-fail + verify.sh CH 权威绿
  - README 八段式规则路径骨架
affects: [04-03, 04-04, 04-05]

tech-stack:
  added: []
  patterns: [Keyed State 特征无 Redis, CH HTTP SinkV2 拒引号, RULE_LABEL 白名单 verify, ai.enabled=false 默认]

key-files:
  created:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/enrich/FeatureEnricher.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/rule/RuleTagger.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/sink/ClickHouseLogSink.java
    - projects/p01-log-ai-platform/scripts/gen_log_events.py
    - projects/p01-log-ai-platform/README.md
  modified:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/LogAiJob.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/model/LogResult.java
    - projects/p01-log-ai-platform/scripts/verify.sh
    - projects/p01-log-ai-platform/Makefile
    - projects/p01-log-ai-platform/pom.xml

key-decisions:
  - "featureJson 用无引号紧凑格式 {errorCount:N,service:…}，避开 Sink 拒双引号与合法 JSON 冲突"
  - "RuleTagger.tag(LogEvent) 供单测；tag(LogResult) 叠加 ERROR_BURST 阈值 5"
  - "本切片不接 Async Ollama；LogAiJob 整图 Parse→Enrich→Rule→CH"

patterns-established:
  - "p01 verify 与 p03 同纪律：CH count 唯一 exit 0；Kafka 仅 diag"
  - "Makefile match = truncate → gen → 轮询 verify"

requirements-completed: [LOG-01, LOG-02]

duration: 4min
completed: 2026-07-18
---

# Phase 04 Plan 02: V2 规则路径 Summary

**默认 AI off 下 Parse→FeatureEnricher→RuleTagger→ClickHouse 端到端可复现：`rule-auth-fail` 造数后 CH `rule_label=AUTH_FAIL` 且 `ai_source=DISABLED`，`make verify` exit 0**

## Performance

- **Duration:** 4 min
- **Started:** 2026-07-18T03:00:45Z
- **Completed:** 2026-07-18T03:04:38Z
- **Tasks:** 3
- **Files modified:** 10

## Accomplishments

- TDD：放开 `RuleTaggerTest` RED → 实现 RuleTagger/FeatureEnricher/ClickHouseLogSink GREEN；`RuleTaggerTest`+`ParseLogJsonTest`+package 绿
- OrbStack e2e：`make up-p01` → `make submit`（`--ai.enabled=false`）→ `make gen`（rule-auth-fail）→ `make verify` exit 0
- TRUNCATE 负例 verify ≠ 0；非法 `RULE_LABEL` 拒绝；README 八段式无违禁词

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: enable RuleTaggerTest** - `0efa484` (test)
2. **Task 1 GREEN: FeatureEnricher + RuleTagger + CH Sink** - `66d2ea1` (feat)
3. **Task 2: gen scenarios + verify.sh GREEN** - `96809c2` (feat)
4. **Task 3: README 八段式骨架** - `c03dfdf` (docs)

**Plan metadata:** `2b208d9` (docs: complete V2 rule path plan)

## Files Created/Modified

- `enrich/FeatureEnricher.java` — service Keyed ValueState ERROR 计数 → featureJson
- `rule/RuleTagger.java` — AUTH_FAIL / ERROR_BURST / NONE
- `sink/ClickHouseLogSink.java` — HTTP SinkV2 写 `flinklab.log_results`
- `LogAiJob.java` — 接线规则路径；去掉 V1 print Sink
- `model/LogResult.java` — 默认 featureJson 无引号
- `scripts/gen_log_events.py` — rule-auth-fail / rule-error-burst + rate/duration
- `scripts/verify.sh` — RULE_LABEL 白名单 + CH 权威
- `Makefile` — gen / verify / match / truncate-results
- `README.md` — 八段式启动+验证
- `pom.xml` — 取消 RuleTaggerTest 排除

## Decisions Made

- featureJson 不用标准 JSON 双引号，改用 `{errorCount:N,service:svc}`，与 Sink `containsForbidden`（拒 `'` `"` `\`）共存
- ERROR_BURST 阈值固定为 5；AUTH_FAIL 优先于 BURST
- V2 不挂 BudgetGate / Guardrail / Async AI（留给 04-03+）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Critical] featureJson 引号与 Sink 校验冲突**
- **Found during:** Task 1（实现 FeatureEnricher）
- **Issue:** 若写入 `{"errorCount":N}`，ClickHouseLogSink 会因双引号拒绝整行
- **Fix:** 紧凑无引号格式 + LogResult 默认值同步；RuleTagger 解析 `errorCount:`
- **Files modified:** FeatureEnricher.java, RuleTagger.java, LogResult.java
- **Verification:** e2e 落库 `ai_source=DISABLED` 且 AUTH_FAIL=2
- **Committed in:** `66d2ea1`

## OrbStack E2E Evidence

| Step | Result |
|------|--------|
| `docker make up-p01` | p01-init exit 0；`logs.events` + `flinklab.log_results` |
| `make submit` JobID | `fcfa5ebe09e92d989045a1b097523438`（`--ai.enabled=false`） |
| `make gen` | `ok scenario=rule-auth-fail events=4` |
| `make verify` | `ok log_results_match=2 rule_label=AUTH_FAIL min_count=1 log_results_total=4`（exit 0） |
| TRUNCATE 后 `verify` | FAIL match=0（exit 1） |
| `RULE_LABEL=DROP_TABLE` | FAIL 非法（exit 1） |
| 再 gen + verify | exit 0；CH `SELECT count() … AUTH_FAIL` → **2** |

CH 样例行（verify 通过时）：

| service | level | rule_label | ai_source | ai_risk | trace_id |
|---------|-------|------------|-----------|---------|----------|
| auth-svc | ERROR | AUTH_FAIL | DISABLED | NONE | tr-auth-fail-1 |
| auth-svc | ERROR | AUTH_FAIL | DISABLED | NONE | tr-auth-fail-2 |

## TDD Gate Compliance

- RED：`0efa484` — 放开 RuleTaggerTest，testCompile 找不到 RuleTagger
- GREEN：`66d2ea1` — 实现后 `mvn -Dtest=RuleTaggerTest,ParseLogJsonTest test` 与 package 成功

## Known Stubs

- `BudgetGateTest` 仍被 pom `testExcludes` 排除（04-03+）
- `scripts/verify_ai.sh` / `drill_ai_degrade.sh` / `loadtest.sh` 仍为 Wave 0 骨架（非本切片范围）
- README 指向 `docs/DEGRADE-CHECKLIST.md`（随 04-03 交付，本切片未创建）

## Threat Flags

无新增未建模威胁面；Sink/verify 白名单落实 T-04-01；主 pom 仍无 Agents/Milvus（T-04-05）。

## Self-Check: PASSED

- FOUND: FeatureEnricher.java / RuleTagger.java / ClickHouseLogSink.java / gen_log_events.py / verify.sh / README.md
- FOUND commits: `0efa484` `66d2ea1` `96809c2` `c03dfdf`
- OrbStack `make verify` exit 0；AUTH_FAIL count=2
