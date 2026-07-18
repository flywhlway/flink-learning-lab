# p01 项目级压测 baseline（OrbStack arm64 实测）

> 方法论子集对齐仓库 [`benchmark/README.md`](../../../benchmark/README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **非** P5 全矩阵；数字仅来自本次 `loadtest.sh` 运行，禁止当作生产 SLA。
> 负载仅打 lab Kafka（`localhost:9094`），禁止当作生产压测工具链。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | 2026-07-18T03:18:15Z |
| 主机 / 架构 | Darwin arm64 |
| 运行时 | OrbStack + docker compose（`docker/docker-compose.yml`） |
| Flink | 2.2.1（镜像与仓库 SSOT 一致） |
| TaskManagers | 2 |
| Slots total / available | 8 / 3 |
| Jobs running | 3（期望含 `p01-log-ai`，默认 `--ai.enabled=false`） |
| Kafka bootstrap（造数） | `localhost:9094` |
| Prometheus | `http://localhost:9090` |
| 作业并行度快照 | 以 Flink UI / compose 当前配置为准（本报告不改并行度） |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | `gen_log_events.py --rate/--duration`（无 k6/JMeter） |
| 配置速率 | 100 events/s |
| 热身 | 30s（**丢弃**，不计入下表） |
| 计量时长 | 90s（墙钟 ≈ 90s） |
| 期望发送量 | 100 × 90 = 9000 |
| service | `loadtest-svc` |
| 事件混合 | 恒定 INFO `loadtest tick`（规则路径吞吐；非 AUTH_FAIL 判定负载） |
| AI | 本轮默认作业 AI off；不调用 Ollama |

## 3. 指标

吞吐口径：配置速率 100 eps；墙钟折算发送速率 ≈ **100.0** eps（期望发送量 / 计量墙钟秒）。
lag/checkpoint/restarts 来自 Prometheus instant query（`job_name=~"p01.*"`）；CH 行为辅助观测。

| 指标 | 值 | 来源 |
|---|---|---|
| 配置 produce rate | 100 eps | loadtest 参数 |
| 墙钟折算 produce rate | 100.0 eps | 9000 / 90s |
| currentEmitEventTimeLag（kafka_log_events source，max） | 8.0 ms | PromQL |
| lastCheckpointDuration（p01 jobs，max） | 61.0 ms | PromQL |
| numRestarts（p01 jobs，max） | 0.0 | PromQL |
| CH log_results 增量（meter 前后） | 9305（4 → 9309） | ClickHouse |

## 4. 结论

- 本轮在 OrbStack arm64 上以 **100 eps × 90s**（热身 30s 已丢弃）跑通项目级压测，并留下上表实测数字。
- Checkpoint 最大耗时 **61.0 ms**；p01 作业 `numRestarts` max=**0.0**（若 >0 须在复盘中解释）。
- Event-time lag（kafka_log_events source max）= **8.0 ms**。
- 复跑：`cd projects/p01-log-ai-platform && make loadtest`（可用 `RATE`/`WARMUP_SEC`/`DURATION_SEC` 覆盖）。
- 全矩阵 / 更高 eps / 倾斜实验留给 P5 `benchmark/`（PROD-01），本文件不扩展。

