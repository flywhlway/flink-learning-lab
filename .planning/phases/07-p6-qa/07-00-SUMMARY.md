---
phase: 07-p6-qa
plan: 00
subsystem: testing
tags: [qa_check, eng_audit, gates, bash, python]

requires:
  - phase: 06-p5
    provides: "生产化交付物与 baseline/bluegreen 证据路径"
provides:
  - "六硬门 qa_check.sh（案例≥100、文档≥30000、mvn hard fail、违禁词含省略）"
  - "独立 eng_audit.sh（ENG-01…04；ENG-04 PHASES soft）"
  - "count_docs.py 分目录文档行数诊断"
affects: [07-01, 07-02, 07-03, QA-01, QA-02, ENG-01, ENG-02, ENG-03, ENG-04]

tech-stack:
  added: []
  patterns: ["六硬门 FAIL 非 0", "eng_audit 独立 + qa_check 末尾调用", "Wave 0 故意先红"]

key-files:
  created:
    - scripts/eng_audit.sh
    - scripts/count_docs.py
  modified:
    - scripts/qa_check.sh
    - scripts/README.md
    - examples/e12-06-streaming-feature/README.md
    - examples/e12-06-streaming-feature/src/main/java/com/flywhl/flinklab/e12/StreamingFeatureJob.java
    - examples/e12-07-agent-quickstart/README.md

key-decisions:
  - "违禁词含「省略」、禁止裸匹配「略」；先清洗 e12-06/07 三处再升词表"
  - "ENG-04 Wave 0 仅断言 CHANGELOG Unreleased；PHASES P6 严格断言延期 07-03"
  - "eng_audit 独立文件，qa_check 末尾调用（D-10 Discretion）"

patterns-established:
  - "Gate-red-first: 升级阈值后本仓故意 FAIL，后续 Wave 用内容推绿"
  - "ENG 终检与 QA 硬门分离入口、单一汇合"

requirements-completed: [ENG-01, ENG-02, ENG-03, ENG-04]
# QA-01 脚本六硬门已落地；全绿验收留给后续 Wave（本波故意红）

duration: 2min
completed: 2026-07-19
---

# Phase 7 Plan 00: Wave 0 门禁先红 Summary

**升级 `qa_check` 为六硬门并落地 `eng_audit`/`count_docs`；清洗「省略」误杀后本仓诚实红（mains=67、md≈9992）。**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-18T16:25:48Z
- **Completed:** 2026-07-18T16:27:55Z
- **Tasks:** 2/2
- **Files modified:** 7

## Accomplishments

- `scripts/qa_check.sh` 升级为六硬门：compose / 违禁词（含「省略」）/ 断链 / mains≥100 / md≥30000 / mvn compile 硬失败
- 清洗 e12-06 README/Java、e12-07 README 三处「省略」假阳性后再升词表
- 新建 `eng_audit.sh`（ENG-01…04；ENG-04 仅 CHANGELOG）与 `count_docs.py`；`qa_check` 末尾调用 eng_audit；`scripts/README.md` 索引更新
- Wave 0 实测故意红：案例 67&lt;100、文档 ≈9992&lt;30000

## Task Commits

1. **Task 1: 清洗存量「省略」并升级 qa_check 六硬门** - `0475fea` (feat)
2. **Task 2: 新建 eng_audit.sh + count_docs.py 并挂入索引** - `756d8e0` (feat)

**Plan metadata:** `04ef50b` (docs: complete Wave 0 gates-red-first plan)

## Files Created/Modified

- `scripts/qa_check.sh` — 六硬门 + 末尾 eng_audit
- `scripts/eng_audit.sh` — ENG-01…04 终检骨架
- `scripts/count_docs.py` — 分目录文档行数诊断（&lt;30000 exit 1）
- `scripts/README.md` — 脚本索引
- `examples/e12-06-streaming-feature/README.md` — 「省略」→「未包含」
- `examples/e12-06-streaming-feature/.../StreamingFeatureJob.java` — 同上
- `examples/e12-07-agent-quickstart/README.md` — 「不可省略」→「必须保留」

## Decisions Made

- 按 D-08 / Pitfall 1：先改写再升词表，禁止加宽 examples 排除列表
- ENG-04 PHASES 严格断言 soft/deferred 至 07-03，避免 Wave 0 先红死锁
- eng_audit 独立文件 + qa_check 调用（单一入口）

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- 本机 `mvn compile` 在阿里云镜像解析 `flink-connector-postgres-cdc:3.6.0` / `flink-connector-jdbc:4.0.0-2.0` 失败，触发 ⑥ 段 hard fail。与 Wave 0「故意红」叠加；不在本 plan 修镜像/依赖（预存环境问题，记入 deferred）。
- `find|wc` 与 `count_docs.py` 行数差约 3 行（换行计数口径），均远低于 30000，不影响门禁语义。

## User Setup Required

None

## Next Phase Readiness

- Wave 1（07-01）：案例扩容 mains 67→≥100，编译绿
- Wave 2/3：文档实质扩写至 ≥30000
- Wave 4（07-03）：清违禁/断链 + PHASES/README 终稿 + ENG-04 严格断言 + 全绿

## Known Stubs

None

## Deferred Issues

- Maven 阿里云镜像缺 CDC/JDBC 构件导致本机 qa_check ⑥ 红（与案例/行数故意红并存）；后续 Wave 在 OrbStack 用可解析仓库复测
- ENG-04 PHASES P6 ✅ 严格断言 → 07-03

## Self-Check: PASSED

- FOUND: scripts/qa_check.sh, scripts/eng_audit.sh, scripts/count_docs.py, scripts/README.md
- FOUND: commits 0475fea, 756d8e0
- VERIFY: eng_audit exit=0；count_docs exit=1；qa_check exit≠0（案例+文档；本机另有 mvn 解析失败）
