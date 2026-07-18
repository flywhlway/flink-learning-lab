---
phase: 04-p01-ai
plan: 03
subsystem: streaming
tags: [log-ai, ollama, async-io, verify-ai, degrade, flink, clickhouse]

requires:
  - phase: 04-p01-ai/02
    provides: RuleTagger/FeatureEnricher/CH Sink、verify 规则路径绿、JobConfig ai.* 字段
provides:
  - OllamaRiskAsyncFunction（Async /api/chat 风险分级 + timeout DEGRADED）
  - LogAiJob ai.enabled 分支 unorderedWaitWithRetry
  - verify_ai.sh + Makefile verify-ai/submit-ai + gen ai-risk-high
  - docs/DEGRADE-CHECKLIST.md 四格勾选
affects: [04-04, 04-05]

tech-stack:
  added: []
  patterns: [Async Ollama bypass, verify vs verify-ai dual track, timeout→DEGRADED]

key-files:
  created:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/ai/OllamaRiskAsyncFunction.java
    - projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md
  modified:
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/LogAiJob.java
    - projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/JobConfig.java
    - projects/p01-log-ai-platform/scripts/verify_ai.sh
    - projects/p01-log-ai-platform/scripts/gen_log_events.py
    - projects/p01-log-ai-platform/Makefile
    - projects/p01-log-ai-platform/README.md
    - projects/p01-log-ai-platform/pom.xml

key-decisions:
  - "默认模型名保留 qwen3:8b；Makefile/submit-ai 默认 AI_MODEL=qwen3.5:9b-mlx 对齐本机 ollama list"
  - "submit-ai 使用独立 group-id=p01-log-ai-verify，避免与默认规则作业争抢消费组"
  - "护栏/预算/指标留给 04-04；本切片仅 Async 旁路 + 双轨验收 + 降级清单"

patterns-established:
  - "ai.enabled=false 整图不挂 Async；true 才 unorderedWaitWithRetry"
  - "verify-ai 前置 curl /api/tags；CH 断言 ai_source=AI 为唯一放行"
  - "DEGRADE-CHECKLIST 四格可勾选且含可执行命令"

requirements-completed: [LOG-03]

duration: 5min
completed: 2026-07-18
---

# Phase 04 Plan 03: V3 AI Bypass Summary

**Async Ollama `/api/chat` 风险分级旁路 + `verify-ai` 双轨验收 + DEGRADE-CHECKLIST：本机 `make verify-ai AI_MODEL=qwen3.5:9b-mlx` 绿，默认 `make verify` 仍零 Ollama 依赖**

## Performance

- **Duration:** 5 min
- **Started:** 2026-07-18T03:05:37Z
- **Completed:** 2026-07-18T03:10:24Z
- **Tasks:** 3
- **Files modified:** 9

## Accomplishments

- `OllamaRiskAsyncFunction`：JDK `HttpClient.sendAsync` → `POST {endpoint}/api/chat`（`stream=false`/`think=false`/`format=json`）；解析失败与 `timeout()` → `ai_source=DEGRADED`/`ai_risk=UNKNOWN`
- `LogAiJob` 仅 `cfg.aiEnabled` 时挂 `AsyncDataStream.unorderedWaitWithRetry`；`JobConfig` 钳制 `ai.capacity≤16`
- OrbStack 实测：`make verify-ai AI_MODEL=qwen3.5:9b-mlx` → `ok ai_source=AI ai_risk_match=12`；回归 `make submit`（AI off）→ `make verify` → `AUTH_FAIL` + `ai_source=DISABLED`
- `docs/DEGRADE-CHECKLIST.md` 四格 + REQ 原文枚举→Async Ollama 选型；README 链接双轨门禁

## Task Commits

Each task was committed atomically:

1. **Task 1: OllamaRiskAsyncFunction + ai.enabled 接线** - `58e3ab6` (feat)
2. **Task 2: verify-ai + ai-risk-high + Makefile** - `c7db94e` (feat)
3. **Task 3: DEGRADE-CHECKLIST + README** - `3abaa5f` (docs)

**Plan metadata:** （本 SUMMARY 提交后填写）

## Files Created/Modified

- `ai/OllamaRiskAsyncFunction.java` - Async 风险分级；message 截断 512；禁止 Future.get()
- `LogAiJob.java` - ai.enabled 分支 unorderedWaitWithRetry
- `JobConfig.java` - capacity/retry 钳制
- `scripts/verify_ai.sh` - Ollama 前置 + CH ai_source/ai_risk 白名单
- `scripts/gen_log_events.py` - scenario `ai-risk-high`
- `Makefile` - `submit-ai` / `verify-ai` / `gen-ai` / `cancel-p01`
- `docs/DEGRADE-CHECKLIST.md` - D-05 四格勾选
- `README.md` - 双轨验证 + 清单链接
- `pom.xml` - 注释去 agents/milvus 字面以免契约 grep 误报

## Decisions Made

- 本机无 `qwen3:8b`，有 `qwen3.5:9b-mlx` / `qwen3.6:35b-a3b`：JobConfig 默认仍写 `qwen3:8b`（文档 SSOT），`make submit-ai`/`verify-ai` 默认 `AI_MODEL=qwen3.5:9b-mlx` 并支持覆写
- `submit-ai` 先 `cancel-p01` 再提交，`--ai.timeout-ms=30000` 覆盖冷启动加载
- 不实现 Agents / Milvus / verify-rag / 护栏预算（明确留给 04-04+）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] pom 注释触发 agents/milvus 契约 grep**
- **Found during:** Task 1 verify
- **Issue:** `(! grep -riE 'flink-agents|milvus' pom.xml)` 命中「禁止 flink-agents / Milvus …」注释
- **Fix:** 注释改为「禁止 Agents Preview 坐标 / 向量库 SDK / CEP」
- **Files modified:** `projects/p01-log-ai-platform/pom.xml`
- **Verification:** 契约 grep 通过；package 成功
- **Committed in:** `58e3ab6`

**2. [Rule 2 - Missing Critical] submit-ai 默认模型对齐本机 list**
- **Found during:** Task 2（user 要求）
- **Issue:** 默认 `qwen3:8b` 本机未 pull，verify-ai 会全 DEGRADED
- **Fix:** Makefile `AI_MODEL ?= qwen3.5:9b-mlx` + README/清单写明 `--ai.model` 覆盖
- **Files modified:** `Makefile`、`README.md`、`DEGRADE-CHECKLIST.md`
- **Verification:** `make verify-ai AI_MODEL=qwen3.5:9b-mlx` 绿
- **Committed in:** `c7db94e` / `3abaa5f`

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 missing critical)
**Impact on plan:** 必要正确性修复；无范围蔓延（未做护栏/预算/RAG）

## Evidence

```text
# verify-ai（本机 2026-07-18）
ok ollama_tags reachable url=http://127.0.0.1:11434
ok ai_source=AI ai_risk_match=12 min_count=1 log_results_total=12
verify-ai ok

# 默认 verify 回归（AI off，事后）
ok log_results_match=7 rule_label=AUTH_FAIL ...
SELECT … WHERE ai_source='DISABLED' AND rule_label='AUTH_FAIL' → 7
```

## Issues Encountered

- Flink list 输出含 `p01-log-ai` 作业；`cancel-p01` awk 解析 ` : ` 分隔符取 JobID，实测可用
- 新 `group-id=p01-log-ai-verify` + earliest 会回放 Kafka 历史，导致 AI 行数 > 单次 gen 事件数；不影响 `≥ MIN_COUNT` 断言

## User Setup Required

**可选（仅 verify-ai）：** 宿主机 `ollama serve`，`ollama list` 有可用模型；无默认名时 `make verify-ai AI_MODEL=<本机模型>`。  
默认 `make verify` **不**需要 Ollama。

## Next Phase Readiness

- LOG-03 可观察 AI 旁路 + 显式降级清单已交付
- 04-04 可接护栏 / 预算熔断 / 自定义指标（Counter）
- 主门禁仍以 `verify` 为准；勿把 Ollama 绑进 CI

## Self-Check: PASSED

- FOUND: `projects/p01-log-ai-platform/src/main/java/com/flywhl/flinklab/p01/ai/OllamaRiskAsyncFunction.java`
- FOUND: `projects/p01-log-ai-platform/scripts/verify_ai.sh`
- FOUND: `projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md`
- FOUND: commits `58e3ab6`, `c7db94e`, `3abaa5f`

---
*Phase: 04-p01-ai*
*Completed: 2026-07-18*
