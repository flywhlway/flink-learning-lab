# p02 项目级压测 baseline（OrbStack arm64 实测）

> 方法论子集对齐仓库 [`benchmark/README.md`](../../../benchmark/README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **非** P5 全矩阵；数字仅来自本次 `loadtest.sh` 运行，禁止当作生产 SLA。
> 负载仅打 lab Kafka（`localhost:9094`），禁止当作生产压测工具链。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | 2026-07-18T04:25:22Z |
| 主机 / 架构 | Darwin arm64 |
| 运行时 | OrbStack + docker compose（`docker/docker-compose.yml`） |
| Flink | 2.2.1（镜像与仓库 SSOT 一致） |
| TaskManagers | 2 |
| Slots total / available | 8 / 2 |
| Jobs running | 4（期望含 `p02-realtime-reco`） |
| Kafka bootstrap（造数） | `localhost:9094` |
| Prometheus | `http://localhost:9090` |
| 作业并行度快照 | 以 Flink UI / compose 当前配置为准（本报告不改并行度） |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | `gen_reco_events.py --rate/--duration`（无 k6/JMeter） |
| 配置速率 | 100 events/s |
| 热身 | 30s（实际发送 2336 条；**丢弃**，不计入下表吞吐主数字） |
| 计量时长 | 90s（墙钟 ≈ 90s） |
| 配置期望发送量 | 100 × 90 = 9000 |
| 计量段实际发送量 | 7046（`gen_reco_events.py` 回报） |
| userId | `u-loadtest` |
| 事件混合 | VIEW/CLICK/CART/BUY 轮转 + 种子 item `i-001`..`i-050` |
| 封顶 | RATE ≤ MAX_RATE=500（T-05-03，仅本地 lab） |

## 3. 指标

吞吐口径：配置速率 100 eps；墙钟折算**实测**发送速率 ≈ **78.29** eps（计量段实际发送量 / 计量墙钟秒）。
lag/checkpoint/restarts 来自 Prometheus instant query（`job_name=~"p02.*"`）；CH 行为辅助观测。
说明：每条行为经 Top-K=5 可写出多行 `reco_results`，故 CH 行增量通常 **高于** 事件数。
备注：Python 造数循环含 sleep 开销，实测 eps 低于配置 100（OrbStack arm64 稳定 Discretion，未下调 RATE）。

| 指标 | 值 | 来源 |
|---|---|---|
| 配置 produce rate | 100 eps | loadtest 参数 |
| 墙钟折算 produce rate（实测） | 78.29 eps | 7046 / 90s |
| currentEmitEventTimeLag（kafka_reco_events source，max） | 6.0 ms | PromQL |
| lastCheckpointDuration（p02 jobs，max） | 91.0 ms | PromQL |
| numRestarts（p02 jobs，max） | 0.0 | PromQL |
| CH reco_results 增量（warmup+measure 前后） | 46910（40 → 46950） | ClickHouse |
| CH 粗写入速率（增量 / 墙钟近似） | 335.07 行/s | ClickHouse |

## 4. 结论

- 本轮在 OrbStack arm64 上以 **配置 100 eps × 90s**（热身 30s；计量段实际发送 7046 条 ≈ **78.29** eps）跑通项目级压测，并留下上表实测数字。
- Checkpoint 最大耗时 **91.0 ms**；p02 作业 `numRestarts` max=**0.0**（若 >0 须在复盘中解释）。
- Event-time lag（kafka_reco_events source max）= **6.0 ms**。
- 复跑：`cd projects/p02-realtime-reco && make loadtest`（可用 `RATE`/`WARMUP_SEC`/`DURATION_SEC` 覆盖）。
- 全矩阵 / 更高 eps / 倾斜实验留给 P5 `benchmark/`（PROD-01），本文件不扩展。
- 恰好 2 条硬演练：本 loadtest + `make drill-redis`（D-12）；杀 TM / 断 Kafka 不挡完成。
