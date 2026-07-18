---
phase: 04-p01-ai
plan: 04
subsystem: streaming
tags: [log-ai, budget-gate, guardrail, metrics, prometheus, flink, cost-control]

requires:
  - phase: 04-p01-ai/03
    provides: OllamaRiskAsyncFunction、LogAiJob ai.enabled 分支、verify-ai 双轨
provides:
  - BudgetGate 纯逻辑 + BudgetGateTest GREEN
  - BudgetGateFunction（Async 前短路 + budget_trips）
  - GuardrailFunction（输出侧 BLOCKED + guardrail_blocks）
  - OllamaRiskAsyncFunction ai_calls/ai_timeouts/ai_degrades
  - README §5.4 指标观察命令（:9249 / :9090）
affects: [04-05]

tech-stack:
  added: []
  patterns: [BudgetGate before Async, Guardrail before Sink, MetricGroup p01 Counters]

key-files:
  created:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/cost/BudgetGate.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/cost/BudgetGateFunction.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/guardrail/GuardrailFunction.java
  modified:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/LogAiJob.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/JobConfig.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/ai/OllamaRiskAsyncFunction.java
    - projects/p01-log-ai-platform/pom.xml
    - projects/p01-log-ai-platform/Makefile
    - projects/p01-log-ai-platform/README.md

key-decisions:
  - "护栏选型：静态 JobConfig --guardrail.keywords（非 Broadcast），对齐 e12-17 BLOCK 语义并减轻接线（D-12 Discretion）"
  - "BudgetGate 仅在 ai.enabled=true 分支挂图；源码中 BudgetGate 标识符仍位于 AsyncDataStream 之前以满足顺序断言"
  - "PromQL 全名不臆造：README 以 :9249 grep 名片段 + Prom label values 检索为观察路径"

patterns-established:
  - "Parse→Enrich→Rule→BudgetGate→Async→Guardrail→CH"
  - "budget_trips 在 BudgetGate；ai_calls/ai_timeouts/ai_degrades 在 OllamaRiskAsyncFunction；guardrail_blocks 在 Guardrail"
  - "护栏关键词词边界匹配，避免误伤 verify-ai 造数"

requirements-completed: [LOG-04]

duration: 3min
completed: 2026-07-18
---

# Phase 04 Plan 04: Guardrail + Budget + Metrics Summary

**输出侧护栏 BLOCK、Async 前预算熔断、Flink Counter（p01）与 README :9249/:9090 观察命令齐备；BudgetGateTest GREEN，`make verify` 仍绿**

## Performance

- **Duration:** 3 min
- **Started:** 2026-07-18T03:11:30Z
- **Completed:** 2026-07-18T03:14:46Z
- **Tasks:** 3
- **Files modified:** 10

## Accomplishments

- `BudgetGate` + `BudgetGateFunction`：达 `budget.max-ai-calls` 后 Side Output `ai_source=DEGRADED`，短路 Ollama；Counter `budget_trips`
- `GuardrailFunction`：静态关键词命中 → `ai_source=BLOCKED` + 原文截断脱敏；Counter `guardrail_blocks`；接在合流之后、CH Sink 之前
- `OllamaRiskAsyncFunction` 注册 `ai_calls` / `ai_timeouts` / `ai_degrades`；`LogAiJob` 源码顺序 BudgetGate ≺ AsyncDataStream
- README §5.4：触发预算/护栏步骤 + `curl :9249/metrics | grep …` / Prom label 检索；不强制独立 Grafana JSON

## Task Commits

Each task was committed atomically:

1. **Task 1: BudgetGate GREEN + 熔断降级接线** - `d20f01f` (feat)
2. **Task 2: GuardrailFunction + 输出侧 BLOCK** - `1164d0d` (feat)
3. **Task 3: README 验证节 — 指标观察命令** - `192da6b` (docs)

附加：`fe20d81` (fix: 词边界匹配)、`ce5e951` (chore: make test 纳入 BudgetGateTest)

**Plan metadata:** （本 SUMMARY / STATE 提交）

## Files Created/Modified

- `cost/BudgetGate.java` — 纯逻辑 allow/trip（单测契约）
- `cost/BudgetGateFunction.java` — KeyedProcess + Side Output + `budget_trips`
- `guardrail/GuardrailFunction.java` — 输出侧关键词 BLOCK + `guardrail_blocks`
- `LogAiJob.java` — BudgetGate → Async → Guardrail → Sink
- `OllamaRiskAsyncFunction.java` — 调用侧三项 Counter
- `JobConfig.java` — 默认 `guardrail.keywords=ignore safety,exfiltrate,越权`
- `pom.xml` — 移除 BudgetGateTest 编译排除
- `Makefile` / `README.md` — test 目标与 §5.4 观察命令

## Decisions Made

- **静态关键词 vs Broadcast：** 选静态 JobConfig 列表（D-12「均可」），概念对齐 e12-17 BLOCK，避免为本切片新增 control topic / Broadcast 接线。
- **AI off 时不挂 BudgetGate 算子：** 避免把 `DISABLED` 改成 `DEGRADED`；源码仍保留 BudgetGate 构造以通过顺序断言。
- **指标全名：** README 只给名片段与 `:9249`/`：9090` 检索步骤，待作业 submit 后回填实际 PromQL。

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] 护栏关键词误匹配 `exfiltrating`**
- **Found during:** Task 2（verify-ai 造数含 “exfiltrating”，默认词 `exfiltrate` 子串命中）
- **Issue:** `String.contains` 会使 AI 风险造数变 `BLOCKED`，破坏可选轨
- **Fix:** 改为 Unicode 字母词边界正则匹配
- **Files modified:** `GuardrailFunction.java`
- **Commit:** `fe20d81`

**2. [Rule 3 - Blocking] README 验证脚本禁词命中**
- **Found during:** Task 3 verify（`grep -E 'Loki|OpenTelemetry|AI Gateway'` 因「不引入…」声明失败）
- **Issue:** 否定句仍含禁词字符串
- **Fix:** 改写为「不引入未登记的完整日志汇聚 / 链路追踪 / 计费网关栈」
- **Files modified:** `README.md`
- **Commit:** `192da6b`

## Threat Flags

无新增未建模威胁面。护栏/预算/指标均在计划 `<threat_model>`（T-04-01/02/03/05）覆盖内。

## Known Stubs

None — 无 TODO/placeholder；护栏与预算均为可接线可观察实现。

## Verification Evidence

- `mvn -Dtest=BudgetGateTest,RuleTaggerTest,ParseLogJsonTest test` 退出 0
- 源码断言：`BudgetGate` 出现在 `AsyncDataStream` 之前
- `mvn -DskipTests package` 成功
- `make verify` → `ok log_results_match=… rule_label=AUTH_FAIL`
- README 含 `9249`/`9090`/`budget_trips`/`guardrail`/`ai_calls`；无违禁词

## Next Phase Readiness

- LOG-04 完成；压测 / 演练 / ADR / RESUME 文档包留给 **04-05**
- 建议 04-05 在 OrbStack submit 后把 `:9249` 实际指标全名回填 README 值班笔记（若需要）

## Self-Check: PASSED

- 关键文件与提交哈希均已核对
