# p02 · 简历陈述页（可复现）

> 每条陈述对应仓库内命令或路径。数字只引用 [baseline.md](baseline.md) / `verify` 实测；禁止空泛形容词。Lab 账号见项目 README（ClickHouse `flinklab` / `flinklab123`）。

## 一句话

在 OrbStack arm64 上交付实时推荐闭环：独立 compose profile `p02`、Keyed State + Redis 双通道在线特征、PostgreSQL 候选目录规则 Top-K，并用脚本断言 ClickHouse 权威出口、Redis 停服仍绿，以及项目级压测 baseline。

## 可复现陈述（动词 → 路径）

| 陈述 | 复现命令 / 路径 |
|------|-----------------|
| 一键起 p02 topic / CH DDL / PG 种子，不影响 default `make up` | `cd docker && make up && make init && make up-p02` |
| 打包并提交推荐作业 | `cd projects/p02-realtime-reco && make package && make submit` |
| 用 CH `reco_results` 断言闭环 | `make gen` → `make verify`（`scripts/verify.sh`） |
| 一键清空造数并轮询 CH | `make match` |
| 演示 Redis 不可用时降级到 STATE_ONLY | `make drill-redis`（`scripts/drill_redis_degrade.sh`） |
| 跑项目级压测并留下 baseline 表 | `make loadtest` → [docs/baseline.md](baseline.md) |
| 说明为何双通道而非纯状态/纯 Redis | [docs/adr/0001-dual-channel-features.md](adr/0001-dual-channel-features.md) |
| 说明 Parse→State→Redis→TopK→Kafka/CH 顺序 | [docs/ARCHITECTURE.md](ARCHITECTURE.md) |

## 验收纪律（面试可答）

1. **权威出口是 ClickHouse**，不是 Kafka / Redis KEYS：`verify.sh` 只认 `flinklab.reco_results`；Kafka/Redis 仅诊断（D-10）。
2. **双通道必齐**：Keyed State 累积 + Redis at-least-once 点查；读失败标 `feature_source=STATE_ONLY`，作业不 FAIL（D-01/D-12）。
3. **Redis 写不假装 exactly-once**：Pipeline + CheckpointedFunction，故障可重复 SET（D-03）。
4. **恰好两条硬演练**：`make loadtest` + `make drill-redis`（D-12）；杀 TM / 断 Kafka 不挡完成。

## 实测数字摘录（仅 baseline）

来源：[docs/baseline.md](baseline.md)（`make loadtest` 于 2026-07-18 OrbStack arm64 写入；**非生产 SLA**）。

| 项 | 值 |
|----|-----|
| 配置负载 | 100 eps × 90s（热身 30s 丢弃） |
| 计量段实际发送 | 7046 条 |
| 墙钟折算 produce rate（实测） | 78.29 eps |
| `lastCheckpointDuration`（p02 jobs，max） | 91.0 ms |
| `numRestarts`（p02 jobs，max） | 0.0 |
| `currentEmitEventTimeLag`（kafka_reco_events source，max） | 6.0 ms |
| CH `reco_results` 增量（warmup+measure） | 46910 |
| CH 粗写入速率 | 335.07 行/s |

复跑：`cd projects/p02-realtime-reco && make loadtest`。

## 技术栈锚点（SSOT）

Flink 2.2.1 · JDK 21 · Kafka connector 5.0.0-2.2 · jedis（与 e07-C7 一致）· Redis 7 · PostgreSQL 16 · ClickHouse 24.8 · Prometheus / Grafana（版本见仓库根 README 矩阵）。架构与交叉引用见 [ARCHITECTURE.md](ARCHITECTURE.md)。
