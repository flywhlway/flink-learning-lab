# Phase 6: P5 生产化 - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-18
**Phase:** 6-P5 生产化
**Mode:** `--auto --chain`
**Areas discussed:** Benchmark 矩阵与驱动, Operator Blue/Green, CI/CD 与单一 GitOps, 规范/题库/看板

---

## Benchmark 矩阵与驱动

| Option | Description | Selected |
|--------|-------------|----------|
| 裁剪可跑通矩阵 + 既有 Python/make 驱动 | e01-J2/e10/p03；1k/5k 必跑；HashMap+RocksDB；无 k6 | ✓ |
| README 全笛卡尔积 + 引入 k6 | 作业×负载×backend×ckpt 全交叉；新镜像 | |
| 仅聚合三项目 baseline | 不做仓库级矩阵，只索引项目级报告 | |

**User's choice:** [auto] 裁剪可跑通矩阵 + 既有 Python/make 驱动（recommended default）
**Notes:** 20k/ForSt 为 stretch/附录；热身 30–60s 须在 baseline 声明；产物 `benchmark/baseline.md`

---

## Operator Blue/Green

| Option | Description | Selected |
|--------|-------------|----------|
| p03 FlinkDeployment + 脚本化时间线 | Operator 1.15 Helm；kubectl/状态/日志证据 | ✓ |
| 三项目各做一遍 Blue/Green | p01/p02/p03 全演练 | |
| 仅 YAML 文档无本机 rollout | 纸面 Operator 说明 | |

**User's choice:** [auto] p03 FlinkDeployment + 脚本化时间线（recommended default）
**Notes:** Autoscaler/Session 可选附录；禁止截图散文验收

---

## CI/CD 与单一 GitOps

| Option | Description | Selected |
|--------|-------------|----------|
| Argo CD + GitHub Actions MVP | Helm manifests → Argo sync；CI 跑 qa_check；arm64 优先 | ✓ |
| Flux + 多架构 buildx 硬门禁 | Flux 路径；必须推多架构镜像 | |
| 双栈 Argo+Flux 对照 | 两套工具都深讲 | |

**User's choice:** [auto] Argo CD + GitHub Actions MVP（recommended default）
**Notes:** 对齐 production/README 与 STACK「拒绝双 GitOps」；buildx 多架构非硬门禁

---

## 规范 / 题库 / 看板

| Option | Description | Selected |
|--------|-------------|----------|
| 3 JSON 看板 + interview≥150 + 完整军规 | Loki/OTel 非硬验收；docs 13/14 登记 | ✓ |
| 看板 + 全套 Loki/OTel 硬验收 | 新拉日志/追踪栈 | |
| 仅扩题库不交看板 JSON | monitoring 继续只有 README | |

**User's choice:** [auto] 3 JSON 看板 + interview≥150 + 完整军规（recommended default）
**Notes:** 看板 = 平台总览 / 作业深潜 / AI 专项；best-practice 与 production 互链

---

## Claude's Discretion

- Helm/Argo/GHA 文件布局与命名、dashboard 查询细节、题目配比、20k/ForSt/Autoscaler 是否实跑附录、压测并行度与吞吐数字（实测填写）

## Deferred Ideas

- Loki/OTel 完整接入、Flux 第二路径、云多区域/多租户、P6 计量终检
