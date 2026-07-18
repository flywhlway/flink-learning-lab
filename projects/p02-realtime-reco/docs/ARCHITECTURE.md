# p02 · 架构短文（行为流 → 双通道特征 → 规则 Top-K）

> 对应教材：[examples/e12-06-streaming-feature](../../../examples/e12-06-streaming-feature/) · [examples/e07-connectors](../../../examples/e07-connectors/)（C7 Redis Pipeline）
> 决策记录：[ADR-0001](adr/0001-dual-channel-features.md) · 压测：[baseline.md](baseline.md)

## 1. 边界

本页描述 **实时推荐样板** 在 OrbStack arm64 上可复现的运行拓扑：用户行为接入、Keyed State + Redis 双通道在线特征、PostgreSQL 候选目录、规则加权 Top-K、Kafka/ClickHouse 双写，以及恰好两条演练入口。不覆盖 ANN/Milvus 向量召回、LLM 重排、P5 Operator Blue/Green、仓库级 `benchmark/` 全矩阵。

## 2. 总图

算子顺序锁定：**Parse → keyBy(userId) → SessionFeature → RedisFeatureWriter → TopKScore → Kafka reco.results + ClickHouse reco_results**。Redis 写失败不得打挂作业；打分读失败必须回落 State 并标 `STATE_ONLY`。

```text
                    ┌─────────────────────────────┐
                    │ gen_reco_events.py          │
                    │  (--scenario | --rate/--dur)│
                    └──────────────┬──────────────┘
                                   │ produce
                                   ▼
                          Kafka reco.events
                          （宿主机 localhost:9094 /
                           作业内 kafka:9092）
                                   │
                                   ▼
                          RealtimeRecoJob（单作业）
                          WM: ooo=5s · uid 前缀 p02-*
                                   │
                    ParseBehaviorJson → keyBy(userId)
                                   │
                    SessionFeatureFunction（Keyed State）
                                   │ FeatureSnapshot
                                   ▼
                    RedisFeatureWriter（jedis Pipeline
                      + CheckpointedFunction）
                      写失败 catch → 主流继续
                                   │
                                   ▼
                    TopKScoreFunction
                      Redis 点查成功 → feature_source=REDIS
                      失败/空 → STATE_ONLY（随流 State）
                      PG reco_items → RuleScorer Top-K=5
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
           Kafka reco.results          CH flinklab.reco_results
           （诊断）                    （权威：user_id/item_id/score/
                                         feature_source/…）
                    │
                    ├── verify.sh / make match（CH 权威）
                    ├── loadtest.sh → docs/baseline.md
                    └── drill_redis_degrade.sh
                        （stop fll-redis → STATE_ONLY）
```

## 3. 验收与演练

| 轨 | 入口 | 前置 | 权威断言 |
|----|------|------|----------|
| 默认闭环 | `make submit` → `make verify` / `make match` | redis/pg/CH 健康 | CH `reco_results` count≥1；可选 `FEATURE_SOURCE=REDIS\|STATE_ONLY` |
| Redis 降级 | `make drill-redis` | 作业 RUNNING；演练前 redis 可 PING | CH `feature_source=STATE_ONLY`；脚本 EXIT 恢复 redis |
| 压测 baseline | `make loadtest` | p02 作业 RUNNING；redis 健康 | [baseline.md](baseline.md)（OrbStack 实测） |

Compose 隔离：`profiles: ["p02"]` 的 `p02-init` 创建 `reco.events` / `reco.results` 与 `flinklab.reco_results` DDL；PG `reco_items` 种子由 `make up-p02` 经宿主机 `psql` 注入。default `make up` **不**要求 `--profile p02`。

双通道选型见 [ADR-0001](adr/0001-dual-channel-features.md)。

## 4. 语义边界

| 能力 | 语义 | 说明 |
|------|------|------|
| Flink checkpoint | EXACTLY_ONCE（作业配置） | 状态与 Kafka sink 对齐仓库基座 |
| Redis 特征写 | **at-least-once** | 非 exactly-once；重复 SET 幂等无害 |
| 打分降级 | STATE_ONLY | 读 Redis 失败不重启作业 |
| 权威出口 | ClickHouse | Kafka / Redis KEYS 仅诊断（D-10） |

## 5. 交叉引用

- 项目 README（八段式）：[../README.md](../README.md)
- 简历陈述页：[RESUME.md](RESUME.md)
- 压测数字：[baseline.md](baseline.md)
- 验证脚本：`scripts/verify.sh` · `scripts/drill_redis_degrade.sh` · `scripts/loadtest.sh`
