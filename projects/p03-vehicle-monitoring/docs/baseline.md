# p03 项目级压测 baseline（OrbStack arm64 实测）

> 方法论子集对齐仓库 [`benchmark/README.md`](../../../benchmark/README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **非** P5 全矩阵；数字仅来自本次 `loadtest.sh` 运行，禁止当作生产 SLA。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | 2026-07-18T02:07:09Z |
| 主机 / 架构 | Darwin arm64 |
| 运行时 | OrbStack + docker compose（`docker/docker-compose.yml`） |
| Flink | 2.2.1（镜像与仓库 SSOT 一致） |
| TaskManagers | 2 |
| Slots total / available | 8 / 6 |
| Jobs running | 2（期望含 `p03_vehicle_alert` + `p03_vehicle_window_metrics`） |
| Kafka bootstrap（造数） | `localhost:9094` |
| Prometheus | `http://localhost:9090` |
| 作业并行度快照 | 以 Flink UI / compose 当前配置为准（本报告不改并行度） |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | `gen_vehicle_events.py --rate/--duration`（无 k6/JMeter） |
| 配置速率 | 100 events/s |
| 热身 | 45s（**丢弃**，不计入下表） |
| 计量时长 | 120s（墙钟 ≈ 120s） |
| 期望发送量 | 100 × 120 = 12000 |
| vin | `VIN-P03-LOAD` × vin-count=8 |
| 事件混合 | ~80% HEARTBEAT / ~20% HARSH_ACCEL，advancing eventTime |
| 乱序 | 造数侧不额外注入乱序；作业 ooo=5s |

## 3. 指标

吞吐口径：配置速率 100 eps；墙钟折算发送速率 ≈ **100.0** eps（期望发送量 / 计量墙钟秒）。
lag/checkpoint/restarts 来自 Prometheus instant query（`job_name=~"p03.*"`）；CH 行为辅助观测。

| 指标 | 值 | 来源 |
|---|---|---|
| 配置 produce rate | 100 eps | loadtest 参数 |
| 墙钟折算 produce rate | 100.0 eps | 12000 / 120s |
| currentEmitEventTimeLag（vehicle_events source，max） | 11.0 ms | PromQL |
| lastCheckpointDuration（p03 jobs，max） | 81.0 ms | PromQL |
| numRestarts（p03 jobs，max） | 0.0 | PromQL |
| window job source lag（avg） | 10.0 ms | PromQL |
| window job lastCheckpointDuration | 39.0 ms | PromQL |
| CH MATCH 增量（meter 前后） | 0（1 → 1） | ClickHouse |
| CH vehicle_window_metrics 增量 | 0（10 → 10） | ClickHouse |

## 4. 结论

- 本轮在 OrbStack arm64 上以 **100 eps × 120s**（热身 45s 已丢弃）跑通项目级压测，并留下上表实测数字。
- Checkpoint 最大耗时 **81.0 ms**；p03 作业 `numRestarts` max=**0.0**（若 >0 须在复盘中解释）。
- Event-time lag（vehicle_events source max）= **11.0 ms**；窗口作业 source lag avg=**10.0 ms**。
- 复跑：`cd projects/p03-vehicle-monitoring && make loadtest`（可用 `RATE`/`WARMUP_SEC`/`DURATION_SEC` 覆盖）。
- 全矩阵 / 更高 eps / 倾斜实验留给 P5 `benchmark/`（PROD-01），本文件不扩展。

