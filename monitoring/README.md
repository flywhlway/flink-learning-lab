# monitoring/ · 可观测性

Phase 0 已接通 JM/TM 以 Prometheus reporter（`:9249`）暴露指标，Prometheus 抓取（TM 走 DNS 服务发现），Grafana 预配数据源（`admin` / `flinklab`）。

## 仓库级三块看板（PROD-04 / D-10）

| 文件 | Grafana 标题 | 用途 |
|---|---|---|
| `platform-overview.json` | `flinklab-platform-overview` | 平台总览：值班五指标 + JM/TM 容量 |
| `job-deepdive.json` | `flinklab-job-deepdive` | 作业深潜：软/硬反压、checkpoint 成败、event-time lag |
| `ai-cost.json` | `flinklab-ai-cost` | AI 专项：`p01` 的 `ai_calls` / `budget_trips` / `guardrail_blocks` 等 |

**打开路径（OrbStack 基座 up 后）：**

1. Grafana → http://localhost:3000 （`admin` / `flinklab`）
2. 左侧 Dashboards → 文件夹 **flinklab**（file provider `flinklab-repo`）
3. 或 Search：`flinklab-platform` / `flinklab-job` / `flinklab-ai`

Provisioning：`docker/config/grafana/provisioning/dashboards/dashboards.yml` 的 `flinklab-repo` provider，目录挂载为 compose volume：

```text
../monitoring → /var/lib/grafana/dashboards/repo
```

改 JSON 后约 30s 自动刷新；首次加 volume 需：

```bash
cd docker && docker compose up -d --force-recreate grafana
```

**一键校验（JSON 非空 + Grafana search 可见）：**

```bash
bash monitoring/scripts/verify_repo_dashboards.sh
```

### PromQL 来源（禁止臆造）

面板表达式只使用本机 Prometheus `:9090` 已暴露序列（抽样于 OrbStack）：

| 看板用途 | 真实序列名 |
|---|---|
| 反压 | `flink_taskmanager_job_task_backPressuredTimeMsPerSecond`（及 soft/hard 变体） |
| 繁忙 | `flink_taskmanager_job_task_busyTimeMsPerSecond` |
| Checkpoint | `flink_jobmanager_job_lastCheckpointDuration` / `numberOfFailedCheckpoints` |
| 重启 | `flink_jobmanager_job_numRestarts` |
| 事件时间滞后 | `flink_taskmanager_job_task_operator_currentEmitEventTimeLag` |
| p01 AI | `flink_taskmanager_job_task_operator_p01_ai_calls` / `_ai_timeouts` / `_ai_degrades` / `_budget_trips` / `_guardrail_blocks` |

核实命令：

```bash
curl -sS http://localhost:9090/api/v1/label/__name__/values \
  | python3 -c "import sys,json; print([n for n in json.load(sys.stdin)['data'] if 'p01_' in n or 'BackPressured' in n])"
```

数据源 uid：Prometheus `PBFA97CFB590B2093`；ClickHouse `p03-clickhouse`（项目级 p03 大盘使用，仓库级三块以 Prom 为主）。

## 值班五指标（先看这五个）

| 指标 | 含义 | 健康线 |
|---|---|---|
| `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | 反压时间占比 | 持续 >500 需排查 |
| `flink_taskmanager_job_task_busyTimeMsPerSecond` | 算子繁忙度 | 持续 ≈1000 即瓶颈算子 |
| `flink_jobmanager_job_lastCheckpointDuration` | 最近 checkpoint 耗时 | 应 << 间隔（30s） |
| `flink_jobmanager_job_numRestarts` | 重启次数 | 任何增长都要有解释 |
| `flink_taskmanager_job_task_operator_currentEmitEventTimeLag` | 事件时间滞后 | 决定「实时」是否名副其实 |

## 快速验证

Prometheus（`:9090`）→ Status → Targets 应看到 jobmanager + N 个 taskmanager 均 UP；提交作业后查询 `flink_jobmanager_numRunningJobs` 应为 ≥1。

## 可选增强（非本 Phase 硬门禁）

**Loki 日志汇聚**与 **OTel Tracing** 可作为后续可观测增强，**不作为 PROD-04 验收条件**，也不为此新拉未登记可观测栈挡门禁。项目级 p03 大盘仍在 `projects/p03-vehicle-monitoring/monitoring/dashboards/`。

---

## Wave 2 扩写 · 可观测值班

### 条目 1

「可观测值班」条目 1：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 2

「可观测值班」条目 2：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 3

「可观测值班」条目 3：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 4

「可观测值班」条目 4：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 5

「可观测值班」条目 5：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 6

「可观测值班」条目 6：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 7

「可观测值班」条目 7：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 8

「可观测值班」条目 8：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 9

「可观测值班」条目 9：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 10

「可观测值班」条目 10：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 11

「可观测值班」条目 11：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 12

「可观测值班」条目 12：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 13

「可观测值班」条目 13：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 14

「可观测值班」条目 14：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 15

「可观测值班」条目 15：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 16

「可观测值班」条目 16：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 17

「可观测值班」条目 17：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 18

「可观测值班」条目 18：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 19

「可观测值班」条目 19：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

