# monitoring/ · 可观测性

Phase 0 已接通:JM/TM 以 Prometheus reporter(:9249)暴露指标,Prometheus 抓取(TM 走 DNS 服务发现),Grafana 预配数据源(admin/flinklab)。

## 值班五指标(先看这五个)

| 指标 | 含义 | 健康线 |
|---|---|---|
| `flink_taskmanager_job_task_isBackPressuredTimeMsPerSecond` | 反压时间占比 | 持续 >500 需排查 |
| `flink_taskmanager_job_task_busyTimeMsPerSecond` | 算子繁忙度 | 持续 ≈1000 即瓶颈算子 |
| `flink_jobmanager_job_lastCheckpointDuration` | 最近 checkpoint 耗时 | 应 << 间隔(30s) |
| `flink_jobmanager_job_numRestarts` | 重启次数 | 任何增长都要有解释 |
| `..._currentEmitEventTimeLag`(source) | 事件时间滞后 | 决定"实时"是否名副其实 |

## 快速验证

Prometheus(:9090)→ Status → Targets 应看到 jobmanager + N 个 taskmanager 均 UP;提交 e01-J2 后查询 `flink_jobmanager_numRunningJobs` 应为 1。

## 规划(P5)

Grafana 看板 JSON 三块(平台总览 / 作业深潜 / AI 专项含 token 成本)、Loki 日志、OTel Tracing 接入 —— 与 production/ 的 K8s 形态共用一套看板定义。
