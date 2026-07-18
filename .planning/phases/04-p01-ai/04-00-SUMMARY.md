---
phase: 04-p01-ai
plan: 00
subsystem: testing
tags: [nyquist-red, junit5, surefire, clickhouse, verify-script, compose-profile, p01]

requires: []
provides:
  - p01 独立 Maven 模块脚手架（surefire + shade，无 Agents/Milvus/CEP）
  - ParseLogJsonTest / RuleTaggerTest / BudgetGateTest Wave 0 RED 夹具
  - verify / verify_ai / drill_ai_degrade / loadtest 失败态骨架
  - flinklab.log_results DDL + p01-init + make up-p01
affects: [04-01, 04-02, 04-03, 04-04, 04-05]

tech-stack:
  added: [flink-streaming-java 2.2.1, flink-connector-kafka 5.0.0-2.2, jackson-databind 2.17.2, junit-jupiter 5.10.2]
  patterns: [Wave 0 RED fixtures, CH-authoritative verify exit, independent projects/ module, --profile p01 isolation]

key-files:
  created:
    - projects/p01-log-ai-platform/pom.xml
    - projects/p01-log-ai-platform/src/test/java/com/flywhl/flinklab/p01/ParseLogJsonTest.java
    - projects/p01-log-ai-platform/src/test/java/com/flywhl/flinklab/p01/rule/RuleTaggerTest.java
    - projects/p01-log-ai-platform/src/test/java/com/flywhl/flinklab/p01/cost/BudgetGateTest.java
    - projects/p01-log-ai-platform/scripts/verify.sh
    - projects/p01-log-ai-platform/scripts/verify_ai.sh
    - projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh
    - projects/p01-log-ai-platform/scripts/loadtest.sh
    - projects/p01-log-ai-platform/sql/clickhouse_log_results.sql
  modified:
    - docker/docker-compose.yml
    - docker/Makefile

key-decisions:
  - "Wave 0 故意引用尚未交付的 ParseLogJson/RuleTagger/BudgetGate，testCompile 失败以建立 RED 反馈环"
  - "四脚本以 --implemented 门闩保持默认非 0；verify 声明 RULE_LABEL 白名单与 CH 权威出口"
  - "p01-init 独立 --profile p01；default make up 不加 p01"
  - "主 pom 禁止 flink-agents / Milvus / flink-cep（D-02/D-03/D-07）"

patterns-established:
  - "Wave 0 RED: 单测可发现但编译失败；脚本 bash -n 通过且直接执行非 0"
  - "projects/p01-log-ai-platform 独立模块，不挂 examples 父工程"
  - "compose profile 钩子镜像 p03-init（KAFKA_IMAGE + wget POST 单语句 DDL）"

requirements-completed: []

duration: 2min
completed: 2026-07-18
---

# Phase 04 Plan 00: p01 Wave 0 Nyquist 夹具 Summary

**独立 p01 Maven 骨架 + Parse/Rule/Budget RED 单测 + 四脚本失败态 + log_results DDL / p01-init / up-p01（不污染 default up）**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-18T02:53:40Z
- **Completed:** 2026-07-18T02:55:14Z
- **Tasks:** 3
- **Files modified:** 11

## Accomplishments

- 落地 `projects/p01-log-ai-platform` 独立模块（Flink 2.2.1、Kafka connector、jackson、surefire、shade → `LogAiJob`）
- 三份 JUnit 锁定解析/AUTH_FAIL/预算熔断契约；生产类未交付 → surefire ≠ 0（Nyquist RED）
- `verify.sh` / `verify_ai.sh` / `drill_ai_degrade.sh` / `loadtest.sh` 可 `bash -n`，默认 exit 1；CH `log_results` + `RULE_LABEL` 白名单意图已写明
- `clickhouse_log_results.sql` + compose `p01-init` + `make up-p01`；`docker compose config -q` 通过；default `up` 无 `--profile p01`

## Task Commits

Each task was committed atomically:

1. **Task 1: RED 夹具 — 最小 pom + Parse/Rule/Budget 单测** - `85ccee0` (test)
2. **Task 2: RED 骨架 — verify / verify-ai / drill / loadtest 四脚本** - `6e92050` (feat)
3. **Task 3: 骨架 — log_results DDL + p01-init + make up-p01** - `d2cc017` (feat)

**Plan metadata:** （见 final docs commit）

## Files Created/Modified

- `projects/p01-log-ai-platform/pom.xml` — 独立模块；禁 Agents/Milvus/CEP
- `projects/p01-log-ai-platform/src/test/java/.../ParseLogJsonTest.java` — 合法 JSON / 缺字段 / 注入字符丢弃
- `projects/p01-log-ai-platform/src/test/java/.../rule/RuleTaggerTest.java` — AUTH_FAIL / NONE
- `projects/p01-log-ai-platform/src/test/java/.../cost/BudgetGateTest.java` — allow / trip
- `projects/p01-log-ai-platform/scripts/verify.sh` — CH 权威 + RULE_LABEL 白名单骨架
- `projects/p01-log-ai-platform/scripts/verify_ai.sh` — Ollama + ai_source 骨架
- `projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh` — AI degrade 演练骨架
- `projects/p01-log-ai-platform/scripts/loadtest.sh` — baseline.md 压测骨架
- `projects/p01-log-ai-platform/sql/clickhouse_log_results.sql` — 单语句 DDL
- `docker/docker-compose.yml` — `p01-init` profile
- `docker/Makefile` — `up-p01` 目标

## Decisions Made

- RED 采用「引用不存在符号 → testCompile 失败」，与计划「不实现生产类」一致，而非 p03 式故意残缺实现。
- 脚本用 `--implemented` 门闩，保证 Wave 0 直接执行恒非 0，同时留下后续绿路径接线点。
- `requirements-completed` 留空：本 plan 仅建立 LOG-01–05 Nyquist 失败态夹具，GREEN 由 04-01+ 交付。

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None — 无外部密钥；后续 `make up-p01` 依赖核心 `make up` 已起的 Kafka/ClickHouse。

## Known Stubs

| Stub | File | Reason |
|------|------|--------|
| 生产类未交付 | ParseLogJson / RuleTagger / BudgetGate / LogEvent / LogAiJob | Wave 0 故意 RED；04-01+ 实现 |
| verify 绿路径 | `scripts/verify.sh` | `--implemented` 尚未接线 CH 查询 |
| verify-ai / drill / loadtest | 对应 scripts | Wave 0 骨架；E2E/baseline 留给后续切片 |

## Threat Flags

无新增信任边界；verify RULE_LABEL 白名单与 pom 禁 Agents/Milvus 已按 T-04-01 / T-04-05 落位。

## TDD Gate Compliance

- RED gate：`85ccee0` `test(04-00): ...` 存在
- GREEN gate：本 plan 明确不实现生产类，无 `feat(04-00): implement ...` — 符合 Wave 0 范围
- 无 REFACTOR commit

## Next Phase Readiness

- 04-01 可实现 `LogEvent` / `ParseLogJson` 使 `ParseLogJsonTest` 转绿
- 04-02+ 接线规则路径、CH Sink、完整 `verify.sh --implemented`
- 勿改 default `make up` 加入 `--profile p01`

## Self-Check: PASSED

- 全部 must-have 文件存在
- 提交 `85ccee0` / `6e92050` / `d2cc017` 均在 git log
- mvn 三测 ≠ 0；四脚本直接执行 ≠ 0；`docker compose config -q` 通过
