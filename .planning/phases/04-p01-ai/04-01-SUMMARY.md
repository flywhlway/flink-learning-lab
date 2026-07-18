---
phase: 04-p01-ai
plan: 01
subsystem: streaming
tags: [log-ai, parse, jobconfig, compose-profile, makefile, kafka, flink]

requires:
  - phase: 04-p01-ai/00
    provides: Wave 0 pom/RED 单测/p01-init/DDL/up-p01
provides:
  - LogEvent/LogResult + JobConfig（ai.enabled 默认 false）
  - ParseLogJson GREEN（tryParse + RichFlatMap）
  - LogAiJob Kafka→Parse 透传可 shade/submit
  - Makefile package/submit + docker submit-p01
  - smoke_p01_profile.sh 配置级 profile 隔离
affects: [04-02, 04-03]

tech-stack:
  added: []
  patterns: [手写 --key JobConfig, Parse 脏数据丢弃, uid 前缀 p01-, profile 隔离 smoke]

key-files:
  created:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/LogAiJob.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/JobConfig.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/ParseLogJson.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/model/LogEvent.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/model/LogResult.java
    - projects/p01-log-ai-platform/Makefile
    - scripts/smoke_p01_profile.sh
  modified:
    - projects/p01-log-ai-platform/pom.xml
    - docker/Makefile

key-decisions:
  - "LogEvent 用公开字段 + 无参构造（Flink POJO）并保留访问器以兼容 Wave 0 单测"
  - "排除 RuleTaggerTest/BudgetGateTest 编译直至 04-02+ 交付生产类"
  - "V1 LogAiJob 以 print 作临时 Sink，注释标明 V2 接 CH"
  - "submit 显式 --ai.enabled=false，与 JobConfig 默认双重保险"

patterns-established:
  - "ParseLogJson.tryParse 纯函数供单测与 flatMap 共用"
  - "scripts/smoke_p01_profile.sh 用 compose config --services 差集断言 D-08"

requirements-completed: [LOG-01]

duration: 3min
completed: 2026-07-18
---

# Phase 04 Plan 01: V1 骨架垂直切片 Summary

**独立 p01 启动面：ParseLogJson GREEN + JobConfig 默认 AI off + LogAiJob 透传可打包提交 + smoke_p01_profile 隔离断言**

## Performance

- **Duration:** 3 min
- **Started:** 2026-07-18T02:57:06Z
- **Completed:** 2026-07-18T02:59:30Z
- **Tasks:** 3
- **Files modified:** 9

## Accomplishments

- Wave 0 `ParseLogJsonTest` 转绿：合法事件保留，缺字段/注入字符丢弃
- `JobConfig` 手写 `--key` 解析，`--ai.enabled` 默认 `false`；AI 端点等字段预留后续切片
- `LogAiJob`：`logs.events` → Parse → WM(ooo=5s,idleness=30s)，算子 `uid("p01-...")`；shade jar + Makefile/docker `submit-p01`
- `smoke_p01_profile.sh` 绿；OrbStack `make up-p01` 实测创建 `logs.events` 与 `flinklab.log_results`

## Task Commits

Each task was committed atomically:

1. **Task 1: 模型 + JobConfig + ParseLogJson GREEN** - `9669f0c` (feat)
2. **Task 2: LogAiJob 透传 + Makefile package/submit** - `c6125cc` (feat)
3. **Task 3: p01-init 冒烟 + profile 隔离断言** - `2974e21` (feat)

**Plan metadata:** （见 final docs commit）

## Files Created/Modified

- `projects/p01-log-ai-platform/src/main/java/.../model/LogEvent.java` — 结构化日志 POJO
- `projects/p01-log-ai-platform/src/main/java/.../model/LogResult.java` — 富化结果默认 DISABLED/NONE
- `projects/p01-log-ai-platform/src/main/java/.../JobConfig.java` — ai.enabled 默认 false
- `projects/p01-log-ai-platform/src/main/java/.../ParseLogJson.java` — tryParse + FlatMap
- `projects/p01-log-ai-platform/src/main/java/.../LogAiJob.java` — Kafka→Parse 透传
- `projects/p01-log-ai-platform/Makefile` — package/test/submit
- `projects/p01-log-ai-platform/pom.xml` — 排除未交付 RED 单测编译
- `docker/Makefile` — submit-p01
- `scripts/smoke_p01_profile.sh` — profile 隔离冒烟

## Decisions Made

- LogEvent 同时满足 Flink 公开字段 POJO 与 Wave 0 `service()` 访问器契约。
- Rule/Budget 单测继续排除编译（对齐 p03 Gate 测试推迟模式），避免阻塞 Parse GREEN。
- V1 用 `print` 保作业图连通；规则落库 / verify 绿留给 04-02。

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] 排除 RuleTaggerTest/BudgetGateTest 编译**
- **Found during:** Task 1
- **Issue:** Wave 0 其余 RED 单测引用未交付类，阻塞 `testCompile` / `ParseLogJsonTest` GREEN
- **Fix:** `maven-compiler-plugin` `testExcludes` 直至 04-02+
- **Files modified:** `projects/p01-log-ai-platform/pom.xml`
- **Commit:** `9669f0c`

**2. [Rule 2 - Missing critical] LogAiJob 临时 print Sink**
- **Found during:** Task 2
- **Issue:** 无 Sink 时流图不连通，无法可靠 `flink run`
- **Fix:** 透传后 `.print().uid("p01-print-await-v2-ch-sink")`，注释标明 V2 接 CH
- **Files modified:** `LogAiJob.java`
- **Commit:** `c6125cc`

## Issues Encountered

None blocking.

## User Setup Required

None — `make up-p01` 依赖核心 `make up`（Kafka/ClickHouse）；本执行已在 OrbStack 实测 p01-init。

## Known Stubs

| Stub | File | Reason |
|------|------|--------|
| print 临时 Sink | `LogAiJob.java` | V1 无 CH；04-02 接 ClickHouseLogSink |
| RuleTagger / BudgetGate | 生产类未交付 | 04-02+；单测已排除编译 |
| verify.sh 绿路径 | `scripts/verify.sh` | 仍 `--implemented` 门闩；04-02 |

## Threat Flags

无新增计划外信任边界；Parse 拒引号反斜杠与 pom 禁 Agents/Milvus 按 T-04-01 / T-04-05 落位。

## TDD Gate Compliance

- RED gate：Wave 0 `85ccee0`（04-00）已建立 ParseLogJsonTest 失败态
- GREEN gate：`9669f0c` `feat(04-01): implement LogEvent/JobConfig/ParseLogJson GREEN`
- 无 REFACTOR commit

## Next Phase Readiness

- 04-02 可接线 FeatureEnricher / RuleTagger / CH Sink 与 `verify.sh --implemented`
- 勿改 default `make up` 加入 `--profile p01`
- 恢复 RuleTaggerTest/BudgetGateTest 编译时需同步交付生产类

## Self-Check: PASSED

- 全部 must-have 文件存在
- 提交 `9669f0c` / `c6125cc` / `2974e21` 均在 git log
- `mvn -Dtest=ParseLogJsonTest` 绿；`mvn package` 产出非 original jar
- `smoke_p01_profile.sh` exit 0；compose profile 隔离成立
