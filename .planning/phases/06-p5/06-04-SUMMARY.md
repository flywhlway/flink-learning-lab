---
phase: 06-p5
plan: 04
subsystem: observability-docs
tags: [grafana, prometheus, interview, best-practice, production, PROD-04]

requires:
  - phase: 06-p5/06-01
    provides: Operator Blue/Green 可观察演练与 production 落地
  - phase: 06-p5/06-03
    provides: Argo GitOps + CI 可复现路径
provides:
  - 仓库级恰好 3 块 Grafana dashboard JSON + provisioning
  - interview ≥150 完整答案与 count_interview 门禁绿
  - best-practice 完整规范体系与 production 双向互链
  - docs 模块 14 / README / PHASES / CHANGELOG P5 收尾
affects: [06-p5 verification, P6 QA, resume statements]

tech-stack:
  added: []
  patterns:
    - Grafana file provider 双目录（p03 + monitoring/repo）
    - PromQL 仅用 :9090 已暴露序列（含 p01 MetricGroup 前缀）
    - interview 按 Level 拆文件 + 行首题号计数
    - best-practice 规范正文 ↔ production 落地清单互链

key-files:
  created:
    - monitoring/platform-overview.json
    - monitoring/job-deepdive.json
    - monitoring/ai-cost.json
    - monitoring/scripts/verify_repo_dashboards.sh
    - interview/L1.md
    - interview/L2.md
    - interview/L3.md
    - interview/L4.md
    - interview/L5.md
    - interview/L6.md
    - interview/L7.md
    - interview/L8.md
    - best-practice/01-architecture-naming.md
    - best-practice/02-uid-savepoint.md
    - best-practice/03-checkpoint-kafka.md
    - best-practice/04-state-ttl.md
    - best-practice/05-backpressure.md
    - best-practice/06-logging-exceptions.md
    - best-practice/07-cicd-gitops.md
    - best-practice/08-ai-degrade.md
  modified:
    - monitoring/README.md
    - docker/config/grafana/provisioning/dashboards/dashboards.yml
    - docker/docker-compose.yml
    - interview/README.md
    - scripts/count_interview.py
    - best-practice/README.md
    - docs/14-production/README.md
    - docs/README.md
    - README.md
    - PHASES.md
    - CHANGELOG.md
    - production/README.md
    - production/docs/gitops-cicd.md
    - production/docs/bluegreen-sop.md

key-decisions:
  - "反压 PromQL 使用本机真实序列 backPressuredTimeMsPerSecond（非 README 旧名 isBackPressuredTimeMsPerSecond）"
  - "AI 成本面板用 p01 Counter 全名 flink_taskmanager_job_task_operator_p01_*，不臆造 token 美元指标"
  - "interview 按 L1–L8 拆文件以承载 ≥150 完整答案；计数仍扫 interview/**/*.md"
  - "Loki/OTel 仅文档标注为可选增强，不挡 PROD-04"

patterns-established:
  - "monitoring/ 恰好 3 个 JSON + flinklab-repo provider + verify_repo_dashboards.sh"
  - "规范在 best-practice/，SOP 在 production/docs/，必须双向链接"

requirements-completed: [PROD-04]

duration: 7min
completed: 2026-07-18
---

# Phase 6 Plan 04: PROD-04 规范/题库/看板收官 Summary

**交付仓库级三块可打开 Grafana 看板、230 题完整答案题库、D-12 规范体系与 docs/PHASES/CHANGELOG 的 P5 完成态回填。**

## Performance

- **Duration:** ~7 min
- **Started:** 2026-07-18T06:19:21Z
- **Completed:** 2026-07-18T06:26:00Z
- **Tasks:** 3/3
- **Files modified:** 30+

## Accomplishments

- Grafana：`platform-overview` / `job-deepdive` / `ai-cost` 经 compose volume + `flinklab-repo` provider 加载；`verify_repo_dashboards.sh` search_hits=3/3
- Interview：L1–L8 共 230 题含完整参考答案；`python3 scripts/count_interview.py` exit 0
- Best-practice：八章规范覆盖 D-12 全主题，并与 `production/docs/*` 双向互链；模块 14 / 根 README / PHASES / CHANGELOG 收尾

## Task Commits

1. **Task 1: 恰好三块 Grafana JSON + provisioning** — `c9b21fa` (feat)
2. **Task 2: interview ≥150 + count_interview 绿** — `cd14bd3` (feat)
3. **Task 3: best-practice 体系 + docs/PHASES/CHANGELOG 收尾** — `2f70ec4` (docs)

**Plan metadata:** （见最终 docs commit）

## Files Created/Modified

- `monitoring/*.json` — 三块看板（Prom uid `PBFA97CFB590B2093`）
- `monitoring/scripts/verify_repo_dashboards.sh` — JSON + Grafana search 门禁
- `docker/.../dashboards.yml` + `docker-compose.yml` — flinklab-repo 挂载
- `interview/L*.md` + `README.md` — 分层题库
- `scripts/count_interview.py` — 分文件计数，阈值 150
- `best-practice/0*.md` — D-12 规范正文
- `docs/14-production/`、`docs/README.md`、`README.md`、`PHASES.md`、`CHANGELOG.md` — 完成态

## Decisions Made

- PromQL 以 OrbStack `:9090` 实时 label 值为准；修正值班反压指标名为 `backPressuredTimeMsPerSecond`
- AI 面板不引入未暴露的 token 计费序列，使用已核实的 `p01_*` Counter
- 题库拆文件避免单 README 过大，保持计数脚本兼容

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] 值班反压指标名与真实 Prom 序列不一致**
- **Found during:** Task 1
- **Issue:** 旧 README/`p03` 面板使用 `…isBackPressuredTimeMsPerSecond`，本机 Prom 无此名
- **Fix:** 仓库级看板与 `monitoring/README.md` 改用 `backPressuredTimeMsPerSecond`（及 soft/hard 变体）
- **Files modified:** `monitoring/*.json`, `monitoring/README.md`
- **Committed in:** `c9b21fa`

**2. [Rule 2 - Correctness] 反例文案触发违禁词扫描风险**
- **Found during:** Task 3
- **Issue:** `08-ai-degrade.md` 反例直接写出「请参考官网」
- **Fix:** 改为描述空洞外链行为，避免字面命中违禁词扫描
- **Files modified:** `best-practice/08-ai-degrade.md`
- **Committed in:** `2f70ec4`

## Auth Gates

None.

## Known Stubs

None — 三块看板含非空 panels；题库每题含「参考答案」正文；规范章均有规则+理由+反例。

## Threat Flags

None beyond plan threat model（dashboard JSON 仅本仓评审；无新网络端点/密钥）。

## Verification Results

| Check | Result |
|---|---|
| `ls monitoring/*.json \| wc -l` = 3 | PASS |
| `verify_repo_dashboards.sh` | PASS search_hits=3/3 |
| `python3 scripts/count_interview.py` | PASS interview_questions=230 |
| best-practice ↔ production 互链 | PASS |
| docs 模块 14 完成态 | PASS |
| `bash scripts/qa_check.sh` | PASS（`== QA PASS ==`）；`mvn compile` 因阿里云镜像缓存缺 e07/e08 依赖告警，属既有离线/镜像问题，非本 plan 引入 |

## Self-Check: PASSED

- FOUND: monitoring 三 JSON、interview L*、best-practice 八章、docs/14
- FOUND commits: c9b21fa, cd14bd3, 2f70ec4
