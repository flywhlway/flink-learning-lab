---
phase: 04-p01-ai
plan: 05
subsystem: docs-ops
tags: [log-ai, loadtest, baseline, drill-degrade, adr, architecture, resume, qa-check]

requires:
  - phase: 04-p01-ai/04
    provides: BudgetGate/Guardrail/指标、verify 双轨、可运行 LogAiJob
provides:
  - loadtest.sh → docs/baseline.md（OrbStack arm64 实测）
  - drill_ai_degrade.sh（AI off + 不可达 endpoint → verify 仍绿）
  - ADR-0001 可降级 AI + ARCHITECTURE + RESUME
  - docs/README 15-01 完成态；CHANGELOG/PHASES；qa_check 绿
affects: [phase-4-complete, p02, P5]

tech-stack:
  added: []
  patterns: [project-level loadtest→baseline, exactly-2-drills, degradable-AI ADR]

key-files:
  created:
    - projects/p01-log-ai-platform/docs/baseline.md
    - projects/p01-log-ai-platform/docs/ARCHITECTURE.md
    - projects/p01-log-ai-platform/docs/RESUME.md
    - projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md
  modified:
    - projects/p01-log-ai-platform/scripts/loadtest.sh
    - projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh
    - projects/p01-log-ai-platform/Makefile
    - projects/p01-log-ai-platform/README.md
    - docs/README.md
    - CHANGELOG.md
    - PHASES.md

key-decisions:
  - "压测默认 RATE=100 / WARMUP=30s / DURATION=90s（OrbStack 稳定可跑通 Discretion）"
  - "drill_ai_degrade 同时施加 --ai.enabled=false 与不可达 ai.endpoint=http://127.0.0.1:9"
  - "恰好 2 条硬演练：loadtest + drill-degrade；杀 TM 仅 README 可选附录"

patterns-established:
  - "loadtest：warmup 丢弃 → measure → Prom/CH 摘要写 baseline 四段结构"
  - "drill-degrade：重提 AI off 作业 → truncate → gen → 轮询 verify，失败非 0"
  - "ADR 主题锁定「AI 路径可降级：主构建零硬依赖 Preview/外部模型」"

requirements-completed: [LOG-05]

duration: 6min
completed: 2026-07-18
---

# Phase 04 Plan 05: V5 演练+文档垂直切片 Summary

**p01 达到与 p03 同等单项目完成态：OrbStack 实测 baseline（100 eps×90s，lag 8ms / ckpt 61ms / restarts 0）、AI 降级演练绿、ADR/ARCHITECTURE/RESUME/15-01 齐全、qa_check 全绿**

## Performance

- **Duration:** 6 min
- **Started:** 2026-07-18T03:16:27Z
- **Completed:** 2026-07-18T03:22:20Z
- **Tasks:** 3
- **Files modified:** 12

## Accomplishments

- `loadtest.sh` 去掉 Wave0 stub：warmup/measure + PromQL（`job_name=~"p01.*"`）+ CH 增量 → 写入 `docs/baseline.md`（OrbStack 实测，非编造）
- `drill_ai_degrade.sh`：`--ai.enabled=false` + `ai.endpoint=http://127.0.0.1:9` 重提作业后 `verify` AUTH_FAIL 仍绿
- Makefile `loadtest` / `drill-degrade` 一键入口；gen 已支持 `--rate`/`--duration`
- ADR-0001 + ARCHITECTURE（Parse→Enrich→Rule→BudgetGate→Async→Guardrail→CH）+ RESUME（可验证动词）
- docs/README 15-01 完成态；CHANGELOG/PHASES 更新；`scripts/qa_check.sh` → `== QA PASS ==`

## Task Commits

Each task was committed atomically:

1. **Task 1: loadtest→baseline + drill_ai_degrade** - `386219c` (feat)
2. **Task 2: ADR + ARCHITECTURE + RESUME + README** - `f31ea9f` (docs)
3. **Task 3: 回填 15-01 + CHANGELOG/PHASES + qa_check** - `59dbe68` (docs)

**Plan metadata:** `a238851` (docs: complete V5 drills and docs plan)

## Files Created/Modified

- `scripts/loadtest.sh` — 项目级压测写 baseline
- `scripts/drill_ai_degrade.sh` — AI 降级硬演练
- `docs/baseline.md` — OrbStack 实测四段表
- `docs/adr/0001-ai-path-degradable.md` — 可降级 AI ADR
- `docs/ARCHITECTURE.md` — 架构短文与总图
- `docs/RESUME.md` — 简历可复现陈述
- `Makefile` / `README.md` — 演练入口与文档链接
- `docs/README.md` / `CHANGELOG.md` / `PHASES.md` — 仓库登记

## Decisions Made

- 压测默认 **100 eps × 90s**（热身 30s）：比 p03 略短以稳定合入，仍写入完整四段 baseline。
- 降级演练同时关闭 AI 并指向不可达 endpoint，覆盖「off」与「不可用」两类前提（D-14）。
- ADR 明确拒绝 Agents Preview 进主 pom、默认硬依赖外部模型、以 ML_PREDICT 作第二主验收路径。

## Deviations from Plan

None - plan executed exactly as written.

（注：`gen_log_events.py` 在 Wave0 已支持 `--rate`/`--duration`，Task 1 未改动该文件，验收 grep 已通过。）

## Issues Encountered

- `qa_check.sh` 中 examples 全仓 `mvn compile` 对 e07/e08 部分依赖解析告警（aliyun 缓存），脚本按既有纪律记 `warn` 仍 `== QA PASS ==`（exit 0）。p01 本模块 `mvn -DskipTests package` 已 GREEN。

## User Setup Required

None — 演练与压测复用既有 OrbStack compose / Flink / Prometheus；可选 `verify-ai` 仍需宿主机 Ollama（非本 plan 硬依赖）。

## OrbStack Evidence

| 项 | 值 |
|----|-----|
| drill-degrade | exit 0：`ok drill_ai_degrade complete`；verify `rule_label=AUTH_FAIL` |
| loadtest | 2026-07-18T03:18:15Z；100 eps × 90s；lag **8.0 ms**；ckpt **61.0 ms**；restarts **0.0**；CH delta **9305** |
| baseline 路径 | `projects/p01-log-ai-platform/docs/baseline.md` |

## Known Stubs

None — Wave0 stub 门闩已从 loadtest/drill 脚本移除；baseline 含实测数字。

## Threat Flags

None beyond plan threat model（loadtest 仅打 lab Kafka；RESUME/baseline 无密钥；ADR 锁定零 Preview 硬依赖）。

## Self-Check: PASSED

- FOUND: `projects/p01-log-ai-platform/docs/baseline.md`
- FOUND: `projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md`
- FOUND: `projects/p01-log-ai-platform/docs/ARCHITECTURE.md`
- FOUND: `projects/p01-log-ai-platform/docs/RESUME.md`
- FOUND: commits `386219c` `f31ea9f` `59dbe68`
