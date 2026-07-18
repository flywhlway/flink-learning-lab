# 仓库级压测 baseline（OrbStack arm64 实测）

> 方法论对齐 [`benchmark/README.md`](./README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **权威路径**（D-03）：本文件；`projects/*/docs/baseline.md` 不替代本报告。
> 数字仅来自本次 `make -C benchmark matrix`（或 dry-run）在 compose Flink 上的刮取，禁止当作生产 SLA。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | 2026-07-18T04:57:58Z |
| 主机 / 架构 | Darwin arm64 |
| 运行时 | OrbStack + docker compose（`docker/docker-compose.yml`），**非** K8s Operator |
| Flink | 2.2.1（镜像与仓库 SSOT 一致） |
| TaskManagers | 2 |
| Slots total / available（开跑前） | 8 / 8 |
| Jobs running（开跑前） | 0 |
| Kafka bootstrap（宿主机造数） | `localhost:9094` |
| Prometheus | `http://localhost:9090` |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | e01: `gen_events.py --eps/--total`；p03: `gen_vehicle_events.py --rate/--duration`；e10 C5: 作业内 `Labs.events` RateLimiter（`--eps`，因 CEP 需 amount 字段） |
| 禁止 | 未登记 HTTP 压测工具 / 未登记压测镜像 |
| 热身 | 45s（**丢弃**，不计入下表） |
| 热身偏差声明 | 学习工程热身 45s，相对方法论「理想 3 分钟」偏短（偏差 135s）；指标仅作本机对照，非稳态 SLA |
| 计量时长 | 60s / 单元格 |
| 必跑负载 | 1k / 5k eps |
| stretch | 20k eps（见 SKIPPED）；ForSt（见 SKIPPED） |

## 3. 指标

吞吐口径：配置 eps；lag/checkpoint/restarts/反压/busy 来自 Prometheus instant query（值班五指标，见 `monitoring/README.md`）。

| # | Job | eps | State | Checkpoint | 配置 eps | emitEventTimeLag (ms) | lastCheckpointDuration (ms) | numRestarts | backpressure (ms/s) | busy (ms/s) |
|---|-----|-----|-------|------------|----------|----------------------|----------------------------|-------------|---------------------|-------------|
| 1 | e01-J2 | 1000 | hashmap | 对齐 30s | 1000 | 29.0 | 55.0 | 0.0 | n/a | 2.0 |
| 2 | e01-J2 | 5000 | hashmap | 对齐 30s | 5000 | 16.0 | 38.0 | 0.0 | n/a | 5.0 |
| 3 | e01-J2 | 1000 | rocksdb | 对齐 30s | 1000 | -4.0 | 78.0 | 0.0 | n/a | 39.0 |
| 4 | e10-C5 | 1000 | hashmap | 对齐 30s | 1000 | -1.0 | 39.0 | 0.0 | n/a | 73.0 |
| 5 | e10-C5 | 5000 | rocksdb | 对齐 30s | 5000 | -1.0 | 9273.0 | 0.0 | n/a | 1000.0 |
| 6 | p03-VehicleAlertJob | 1000 | hashmap | 对齐 30s | 1000 | 9753526.0 | 2070.0 | 0.0 | n/a | 982.0 |
| 7 | p03-VehicleAlertJob | 5000 | rocksdb | 对齐 30s | 5000 | 9874851.0 | 98.0 | 0.0 | n/a | 1000.0 |
| 8 | e01-J2 | 1000 | hashmap | 对齐 10s | 1000 | 22.0 | 43.0 | 0.0 | n/a | 10.0 |
| 9 | e01-J2 | 1000 | hashmap | 非对齐 30s | 1000 | 22.0 | 27.0 | 0.0 | n/a | 3.0 |

### SKIPPED

- 20k eps stretch：OrbStack 本机稳定性与 p03 `gen_vehicle_events` `MAX_RATE=5000` 上限——本轮 SKIPPED，禁止填假数
- ForSt state backend：非 D-01 硬门禁，本轮 SKIPPED（可选附录）

## 4. 结论

- 本轮在 OrbStack arm64 / compose Flink 上跑通 D-01 裁剪必跑集（三作业 × 1k/5k × HashMap/RocksDB 增量主路径 + checkpoint 对照行）。
- 单元格 #8（对齐 10s）首轮 Prom 瞬时空结果后已同配置补刮（lag=22 / ckpt=43ms）；harness 已加重试。
- p03 单元格 `emitEventTimeLag` 量级偏大：作业默认 `earliest` 消费叠加历史 backlog，属本轮实测观测，非填假数。
- 复跑：`cd docker && make up && make up-p03` → 打包 e01/e10/p03 → `make -C benchmark matrix`。
- dry-run 冒烟：`make -C benchmark dry-run`（低 EPS 单单元格）。
- 项目级 baseline 可交叉引用本文件，**不**替代仓库级矩阵报告（D-03）。

