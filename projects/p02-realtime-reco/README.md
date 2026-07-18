# p02 · 实时推荐（双通道在线特征 + 规则 Top-K）

> 对应企业实战模块 15-02 · GSD Phase 5（RECO-01–RECO-03）
> 目录锁定：`projects/p02-realtime-reco`。
> 文档包：[ARCHITECTURE](docs/ARCHITECTURE.md) · [RESUME](docs/RESUME.md) · [ADR-0001](docs/adr/0001-dual-channel-features.md) · [baseline](docs/baseline.md)

## 1. 背景

电商/内容场景需要把用户行为流实时转成可解释的 Top-K 候选，并落到可断言的权威表。本项目在 OrbStack arm64 上交付完整闭环：**行为 JSON → Keyed State 会话特征 → Redis 在线特征库（at-least-once）→ PostgreSQL 候选目录规则打分 → Kafka + ClickHouse 双写**。

硬验收：

- 独立 `make up-p02` 不影响 default `make up`
- 造数 `feature-score` 后 CH `flinklab.reco_results` 有行；`verify.sh` **以 ClickHouse 为唯一权威出口**；Kafka / Redis KEYS 仅诊断
- Redis 停服时作业凭 State 通道仍产出 `feature_source=STATE_ONLY`（`make drill-redis`）
- 项目级 `make loadtest` 写入 `docs/baseline.md` 实测数字（非 `benchmark/` 全矩阵）
- 主 pom 独立模块，不挂 `examples/` 父工程；不引入 LLM 重排 / Milvus ANN 作为硬依赖

## 2. 架构

```text
[gen_reco_events.py] --produce--> Kafka reco.events (宿主机 localhost:9094)
                                            |
                               Flink JobManager / TaskManager
                                            |
                               RealtimeRecoJob
         Parse → keyBy(userId) → SessionFeature(Keyed State)
                              → RedisFeatureWriter(jedis Pipeline)
                              → TopKScore(REDIS | STATE_ONLY)
                                            |
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
            Kafka reco.results                          CH reco_results
            （诊断）                                    （权威出口）
                     |
        verify / match · drill-redis · loadtest→baseline
```

Compose 隔离：`profiles: ["p02"]` 的 `p02-init` 幂等创建 topic + CH DDL；PG `reco_items` 由 `make up-p02` 经 `psql` 种子。双通道决策见 [ADR-0001](docs/adr/0001-dual-channel-features.md)；总图见 [ARCHITECTURE](docs/ARCHITECTURE.md)。

## 3. 代码

| 路径 | 职责 |
|---|---|
| `RealtimeRecoJob` | Kafka → Parse → SessionFeature → RedisWriter → TopK → Kafka/CH 双写 |
| `JobConfig` | 手写 `--key`；bootstrap / redis / jdbc / topK / checkpoint |
| `ParseBehaviorJson` | JSON → `BehaviorEvent`；脏数据丢弃；`eventType`∈VIEW\|CLICK\|CART\|BUY |
| `feature/SessionFeatureFunction` | Keyed MapState 累积类目/商品亲和与 clickCount → `FeatureSnapshot` |
| `feature/RedisFeatureWriter` | jedis Pipeline + CheckpointedFunction；at-least-once；写失败不抛 |
| `catalog/CatalogLoader` | JDBC 加载 PG `reco_items`；显式 `Class.forName` |
| `score/RuleScorer` + `TopKScoreFunction` | 规则加权 Top-K=5；`feature_source=REDIS\|STATE_ONLY` |
| `sink/ClickHouseRecoSink` | HTTP SinkV2 → `flinklab.reco_results`；白名单 feature_source |
| `scripts/gen_reco_events.py` | `feature-score` + `--rate/--duration`（RATE≤5000） |
| `scripts/verify.sh` | `FEATURE_SOURCE` 白名单 + CH COUNT（默认门禁） |
| `scripts/drill_redis_degrade.sh` | stop `fll-redis` → CH `STATE_ONLY` → 恢复 redis |
| `scripts/loadtest.sh` | 压测 → `docs/baseline.md` |

Maven 独立模块，版本对齐仓库 SSOT（Flink 2.2.1 / Kafka connector 5.0.0-2.2 / jedis）。

## 4. 启动

前置：OrbStack arm64，本机已安装 JDK 21、Maven、uv、Docker。若宿主机 5432/6379 被占用，见 `docker/.env` 的 `PG_HOST_PORT` / `REDIS_HOST_PORT`（容器内主机名仍为 `postgres` / `redis`）。

```bash
# 1) 基座（不含 p02 profile）
cd docker
make up
make init

# 2) p02 topics + ClickHouse DDL + PG reco_items 种子
make up-p02

# 3) 打包并提交作业
cd ../projects/p02-realtime-reco
make package
make submit

# 4) 可判定造数（特征/打分）
make gen
```

端口速查：Flink UI `http://localhost:8081`；Kafka 宿主机 `localhost:9094`（容器内作业用 `kafka:9092`，勿混用）；ClickHouse `localhost:8123`（`flinklab` / `flinklab123`）；Redis / PG 映射端口以 `docker/.env` 为准。

## 5. 验证

### 5.1 主门禁

权威出口是 ClickHouse，不是 Kafka / Redis。

```bash
cd projects/p02-realtime-reco

make verify
# 期望：ok reco_results_match=N feature_source=ANY ...（exit 0）

make match          # truncate → cancel → submit → gen → 轮询 CH

# 负例：清空后必须非 0
make truncate-results
bash scripts/verify.sh   # 期望 FAIL … count >= 1, got match=0
```

可选按源过滤（白名单，禁止任意拼 SQL）：

```bash
FEATURE_SOURCE=REDIS bash scripts/verify.sh
FEATURE_SOURCE=STATE_ONLY bash scripts/verify.sh
```

### 5.2 两条演练（RECO-03 / D-12）

恰好两条可执行演练；失败非 0。**不**把杀 TaskManager / 断 Kafka 作为完成门禁。

```bash
cd projects/p02-realtime-reco

# (1) Redis 停服 → CH 仍出现 feature_source=STATE_ONLY；脚本 EXIT 恢复 redis
make drill-redis

# (2) 项目级压测 → 写入 docs/baseline.md（OrbStack 实测数字）
make loadtest
# 可选覆盖：make loadtest RATE=50 WARMUP_SEC=20 DURATION_SEC=60
```

可选附录（不挡完成）：在 Flink UI 杀 TaskManager 后观察 checkpoint 恢复；本项目不以杀 TM 作为硬验收。

### 5.3 指标观察

业务 Counter 片段：`p02_redis_write_failures`、`p02_redis_read_failures`、`p02_score_state_only`、`p02_score_redis`。经 Flink Prometheus reporter（TM `:9249`）→ Prometheus `:9090`。完整指标名以 submit 后 `:9249` 实际输出为准。

```bash
curl -sS http://localhost:9249/metrics | grep -E 'p02_redis|p02_score' || true
```

## 6. 踩坑

| 现象 | 原因 | 处理 |
|---|---|---|
| `verify` 一直 match=0 | 作业未 RUNNING / 未造数 / catalog 空 | Flink UI 看 Job；确认 `make up-p02` 已种子 PG；等 10–30s 再 verify |
| `No suitable driver` / catalog 空 | Shade 后 JDBC 驱动未加载 | `CatalogLoader` 已 `Class.forName`；TopK `ensureCatalog` 懒重试 |
| Redis 停服作业 RESTARTING | 写/读路径抛异常 | 必须 catch；见 ADR；`make drill-redis` 会检测并可选重提 |
| 宿主机连不上 Kafka | 用了容器内地址 | 造数 `--bootstrap localhost:9094`；作业内 `kafka:9092` |
| `fll-postgres`/`fll-redis` 起不来 | 5432/6379 被其他栈占用 | `docker/.env` 设 `PG_HOST_PORT`/`REDIS_HOST_PORT`（如 15432/16379） |
| `make up` 被拖慢 | 误把 p02 绑进 default | 只用 `make up-p02`；default `up` 不含 `--profile p02` |
| CH DDL 失败 | HTTP 禁多语句 | `sql/clickhouse_reco_results.sql` 保持单条 CREATE |

## 7. 最佳实践

- **验收权威在 CH**：任何「Kafka 有消息」或「Redis 有 key」只能当诊断，不能 `exit 0`
- **双通道都要可观察**：State 供降级；Redis 供点查叙事；见 ADR-0001
- **Redis 写 at-least-once**：文档与面试主动说明，禁止假装 exactly-once
- **打分读失败不抛**：`STATE_ONLY` + Counter；作业保持 RUNNING
- **FEATURE_SOURCE 白名单**：仅 `REDIS`/`STATE_ONLY` 可拼进 SQL（T-05-01）
- **算子稳定 uid**：`p02-*` 前缀，便于后续 savepoint / 扩缩
- **profile 隔离**：项目 DDL/topic 走 `p02-init`；Redis/PG 复用基座
- **压测封顶**：`loadtest` `MAX_RATE=500`；仅本地 lab（T-05-03）

## 8. 面试题与参考

**Q1：为什么必须双通道，而不是只选 State 或只选 Redis？**  
A：纯 State 无法演示在线特征库点查与跨作业共享；纯 Redis 在停服演练时无特征可打分，违反 D-12。双通道让正常路径标 `REDIS`、降级路径标 `STATE_ONLY`，新鲜度与故障边界可讲清（见 ADR-0001）。

**Q2：Redis 侧是 exactly-once 吗？**  
A：不是。`RedisFeatureWriter` 用 CheckpointedFunction 攒批，checkpoint 尾巴进 Operator ListState，故障恢复可能重复 SET；同 key 幂等无害。作业 checkpoint 配置的 EXACTLY_ONCE 不自动覆盖 Redis 外存语义。

**Q3：`drill-redis` 如何证明降级，而不是「Redis KEYS 为空」？**  
A：脚本 `compose stop redis` 后造数，轮询 `FEATURE_SOURCE=STATE_ONLY` 的 CH count；成功条件是 ClickHouse 行，禁止用 KEYS 扫描放行。EXIT trap 恢复 redis，避免污染 loadtest。

**Q4：verify 如何防 SQL 注入式环境变量？**  
A：`FEATURE_SOURCE` 先走白名单 `case`（仅 `REDIS`/`STATE_ONLY`），非法立即非 0；只有枚举常量拼进 `WHERE`。

**Q5：Top-K 候选从哪来？为何不用向量召回？**  
A：MVP 用 PostgreSQL 合成维表 `reco_items`（约 50 行）+ 规则加权；Milvus/ANN / LLM 重排属后续能力，不挡本 Phase 完成（D-04/D-05）。

参考（仓库内）：

- 架构短文：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 简历陈述页：[docs/RESUME.md](docs/RESUME.md)
- ADR（双通道）：[docs/adr/0001-dual-channel-features.md](docs/adr/0001-dual-channel-features.md)
- 压测 baseline：[docs/baseline.md](docs/baseline.md)
- 可观测基座：[monitoring/README.md](../../monitoring/README.md)（:9249 / :9090）
- 基座版本矩阵与端口：[根 README](../../README.md)
- Compose profile / p02-init：`docker/docker-compose.yml`、`docker/Makefile` 的 `up-p02`
- 双通道样板：`examples/e12-06-streaming-feature/`、`examples/e07-connectors/`（C7）
