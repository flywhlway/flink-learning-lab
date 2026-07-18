# Phase 5: p02 实时推荐 - Research

**Researched:** 2026-07-18
**Domain:** Flink DataStream 实时推荐（行为流 → 双通道在线特征 → 规则召回/打分 → Kafka/ClickHouse）
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### 在线特征存储（RECO-02）
- **D-01:** 在线特征采用 **双通道**（对齐 e12-06 / ai/06）：**(1) Flink Keyed State** 承载会话/窗口内实时累积特征；(2) **Redis**（基座已有 `redis:7-alpine` + e07-C7 jedis Pipeline）作为在线特征库点查侧，供打分阶段低延迟读取。禁止「只写 Redis 不建状态」或「纯状态从不落 Redis」——两者都要可观察，才能讲清新鲜度差异。
- **D-02:** Redis 客户端锁定 **jedis**（与 e07-C7 / e12-06 一致）；版本须先登记根 README 版本矩阵 + 项目/父 pom，禁止漂移到未登记的 lettuce 除非调研证明必须且登记后使用。特征 key 约定形如 `feature:{userId}:*`（可由 planner 微调，须在文档与 verify 中一致）。
- **D-03:** 特征写入须考虑 **at-least-once + 攒批**（复用 e07-C7 CheckpointedFunction/Pipeline 思路）；文档写明 Redis 侧语义与故障边界，禁止假装 exactly-once。

#### 召回 / 打分闭环（RECO-02）
- **D-04:** 闭环锁定为 **规则/简单加权打分**（行为亲和、近因、类目匹配等可解释因子）→ **Top-K 候选** 写出。**不**引入 LLM 重排、**不**引入外部模型服务、**不**以 Milvus/ANN 向量召回作为本 Phase 硬依赖（向量路径属 p01/FUT）。
- **D-05:** 候选目录（item catalog）MVP：**合成静态目录**（作业内常量或 `p02-init` 写入 **PostgreSQL** 维表二选一；推荐 PG 维表 + Async/维表关联以便简历可讲「流批维表」）。目录规模学习工程级（数十～数百 item 即可），禁止接真实广告/电商生产数据。
- **D-06:** 输出契约：每条推荐结果至少含 `userId`、`itemId`（或 item 列表/Top-K JSON）、`score`、`ts`、可选 `reason`/`featureSnapshot` 摘要；写入 **Kafka 推荐结果 topic**（建议 `reco.results`）并 **双写 ClickHouse 权威表**（表名可由 planner 定，如 `reco_results`）。

#### 工程骨架与权威出口（RECO-01）
- **D-07:** 目录与模块名锁定 **`projects/p02-realtime-reco`**（与 `docs/README.md` 模块 **15-02** 登记一致）；**独立 pom**，不挂入 `examples/` 父工程。
- **D-08:** Compose：**独立 `--profile p02`**（topic 初始化 / `p02-init` / 项目专用挂载）；**禁止**污染 default `make up`。Redis/PG 已在 default 基座，p02 profile 只追加项目专属 init/topic，不重复造 Redis 服务。
- **D-09:** 事件契约：合成 **用户行为 JSON**（至少含 `userId`、`itemId`、`eventType`（view/click/cart/buy 或等价枚举）、事件时间戳）；Kafka topic 建议 `reco.events`（幂等创建）；造数脚本提供可判定 scenario（特征可见 + Top-K 结果可断言）。
- **D-10:** **ClickHouse 为权威验收出口**（推荐结果表行数/样例断言）；Kafka 与 Redis KEYS 仅诊断。延续 p03/p01 verify 纪律：不以「Kafka 有消息」或「Redis 有 key」单独放行。

#### 压测、演练与文档包（RECO-03）
- **D-11:** 压测为 **p02 项目级**（非 `benchmark/` 全矩阵）：造数 `--rate/--duration` + `loadtest` → `docs/baseline.md`；口径对齐 p03/p01 / `benchmark/README.md` 子集；数字仅 OrbStack arm64 实测。
- **D-12:** 可执行演练 **恰好 2 条**（MVP）：(1) **Redis 不可用/中断时降级**——作业仍可凭 Keyed State 特征产出可观察推荐结果（或明确降级到「state-only 打分」路径，文档+脚本可断言）；(2) **负载压测跑 baseline**。额外 chaos（杀 TM、断 Kafka）可写可选附录，不挡 Phase 完成。
- **D-13:** 文档包：至少 **1 篇 ADR**（主题锁定 **「在线特征双通道：Flink Keyed State + Redis 点查」vs 纯状态或纯外存**）；`docs/ARCHITECTURE.md` + `docs/RESUME.md`；回填 `docs/README.md` 模块 **15-02** 完成态；README 八段式。验证脚本 + make 目标一键可跟。

### Claude's Discretion
- 具体 CH/PG 表名与列设计、Top-K 默认 K、打分权重公式细节、行为 eventType 枚举命名、是否单独 p02 Grafana dashboard JSON（可复用 Prometheus + CH 查询）、Redis 连接参数默认值。
- 压测默认 eps/时长由 executor 在 OrbStack 可稳定跑通前提下实测填写。

### Deferred Ideas (OUT OF SCOPE)
- ANN / Milvus 向量召回作为第二召回通道 — 属 FUT / 可与 p01 向量能力交叉；本 Phase 不挡
- LLM 重排 / 生成式推荐 — 超出 RECO MVP；STACK 已声明非必须
- 真实广告/电商生产数据接入 — 合规与范围；用合成数据
- 仓库级 monitoring 三块看板终稿、benchmark 全矩阵 — Phase 6（PROD-*）
- Operator Blue/Green 以 p02 作业为对象 — Phase 6
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| RECO-01 | 维护者可用独立 compose profile 一键启动 p02，用户行为流可接入 | 复制 p01/p03：`p02-init` + `make up-p02`；topics `reco.events`/`reco.results`；合成行为 JSON + `gen_reco_events.py` |
| RECO-02 | 在线特征（Keyed State 或 Redis）+ 召回/打分闭环，结果可在 Kafka/存储中观察 | D-01 收紧为双通道必齐；e12-06 + e07-C7 + 规则 Top-K；CH `reco_results` 权威 + Kafka 诊断 |
| RECO-03 | 交付压测、故障演练、架构文档、ADR、验证脚本与简历陈述页 | 项目级 loadtest + 恰好 2 drills；ADR 双通道；ARCHITECTURE/RESUME；docs/README 15-02 回填 |
</phase_requirements>

## Summary

Phase 5 交付第三个 P4 生产级项目 `projects/p02-realtime-reco`。工程纪律应 **逐项复制 p01**（最近完成样板）与 **p03**（原始纪律源）：独立 pom、`--profile p02` init、JobConfig 手写参数、CH 权威 verify、项目级 loadtest、恰好 2 条可执行演练、ADR + ARCHITECTURE + RESUME、八段式 README。域能力上把教材 e12-06 / ai/06 的「窗口/状态特征 → Redis」升格为 **生产作业内闭环**：Keyed State 驱动实时打分，Redis 作可观察在线特征库（at-least-once 攒批写），候选来自 PG 合成维表，规则加权 Top-K 双写 `reco.results` + ClickHouse。

关键技术风险不在新框架，而在 **Redis 降级演练的算子设计**：写 Redis 失败不得打挂作业；打分必须能走 state-only 路径且 CH 仍可断言。另需在根 README 版本矩阵 **首次登记 jedis 5.2.0**（当前仅在 `examples/pom.xml` 属性区，根矩阵缺行）。[VERIFIED: examples/pom.xml + Maven Central jedis:5.2.0]

**Primary recommendation:** 以 p01 目录/脚本/Makefile 为骨架复制到 `p02-realtime-reco`；作业图 = Parse → Keyed Feature（MapState/窗口）→ Redis 攒批写（可降级）→ Catalog 加载 + Rule Scorer Top-K → Kafka + CH SinkV2；verify 只认 CH。

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| 行为事件接入 / topic 初始化 | Docker Compose (`p02-init`) | Kafka | profile 隔离；不进 default `make up` |
| 解析与事件时间 | Flink Job (DataStream) | — | POJO + watermark；与 p01 Parse 同形 |
| 会话/窗口在线特征 | Flink Keyed State | — | D-01 通道一；打分主输入与降级兜底 |
| Redis 特征库写入/点查 | Flink 算子 + Redis | — | D-01 通道二；at-least-once；诊断可观察 |
| Item 候选目录 | PostgreSQL 维表 | Job `open()` 内存加载 | D-05 推荐；流批维表叙事；百级 item 无需每条 Async |
| 规则召回 / Top-K 打分 | Flink Job (纯函数+状态特征) | — | D-04；禁止外部模型 |
| 结果可观察出口 | ClickHouse（权威） | Kafka `reco.results` | D-10；verify 只认 CH |
| 压测 / Redis 降级演练 | Bash scripts + Docker | Flink REST | D-11/D-12；对齐 p01 drill |
| 文档 / ADR / 简历页 | 项目 `docs/` | `docs/README.md` 15-02 | D-13 |

## Project Constraints (from .cursor/rules/)

`.cursor/rules/` 不存在。约束以仓库 `CLAUDE.md` / PROJECT 不变量为准：

- OrbStack arm64 实测通过才合入；禁止伪跑通
- 版本 SSOT：根 README 矩阵 + `examples/pom.xml` 属性区；新增组件先登记
- 文档八段式；禁止 TODO / 省略 / 「请参考官网」
- Phase 结束跑 `scripts/qa_check.sh`
- Tech：Flink 2.2.1、JDK 21、Kafka connector 5.0.0-2.2

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Apache Flink | 2.2.1 | 作业运行时 | ADR-001 / 版本矩阵 SSOT [VERIFIED: README.md] |
| JDK | 21 | 编译与运行 | `flink:2.2.1-java21` [VERIFIED: README.md] |
| flink-connector-kafka | 5.0.0-2.2 | 行为/结果 IO | 官方兼容 2.2.x [VERIFIED: README.md] |
| jedis | **5.2.0** | Redis Pipeline 特征写 | 与 e07/e12 一致；勿升 6.x 除非全仓登记 [VERIFIED: examples/pom.xml + Maven Central] |
| postgresql JDBC | 42.7.4 | 加载 `reco_items` 维表 | `examples/pom.xml` 已管 [VERIFIED: examples/pom.xml] |
| jackson-databind | 2.17.2 | 行为/结果 JSON | p01 同款 [VERIFIED: p01 pom] |
| JUnit Jupiter | 5.10.2 | 打分/解析单测 | 父属性已管 [VERIFIED: examples/pom.xml] |

### Supporting

| Library / Service | Version | Purpose | When to Use |
|-------------------|---------|---------|-------------|
| Redis | redis:7-alpine | 在线特征库 | default 基座；p02 不新建服务 [VERIFIED: docker/.env] |
| PostgreSQL | 16-alpine | item catalog 维表 | default 基座；`p02-init`/Makefile 种子数据 [VERIFIED: docker/.env] |
| ClickHouse | 24.8 | `reco_results` 权威表 | HTTP SinkV2（p01/p03/e07-C6 模式）[VERIFIED: README.md] |
| Kafka | 3.9.1 KRaft | `reco.events` / `reco.results` | 基座 [VERIFIED: README / STACK] |
| uv + confluent-kafka | 与 p01 gen 一致 | 造数 / loadtest | 项目脚本 [VERIFIED: p01 gen_log_events.py] |
| flink-metrics-dropwizard | 父 pom 已管（可选） | `redis_write_fail` / `score_emit` 等业务指标 | 演练可观察时再加 |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| jedis 5.2.0 | lettuce / jedis 6.0.0 | D-02 禁止未登记漂移；6.x 为 Central latest 但破坏 SSOT |
| PG 维表 | 作业内常量 catalog | 常量更快落地但简历弱于「流批维表」；D-05 推荐 PG |
| 每条 Async 查 PG | `open()` 一次加载全表 | 百级 item Top-K 适合全表内存打分；Async 留给 Redis 点查/未来扩展 [CITED: Flink 2.2 Async I/O docs] |
| Redis 作打分唯一输入 | State-only 打分 | 违反 D-01「两者都要」；降级时才允许 state-only |
| Milvus/ANN 召回 | — | Deferred；本 Phase 禁止硬依赖 |

**Installation（项目 pom 属性对齐 SSOT，勿另起版本号）：**

```xml
<!-- projects/p02-realtime-reco/pom.xml properties — 数值抄 examples/pom.xml -->
<flink.version>2.2.1</flink.version>
<flink.kafka.connector.version>5.0.0-2.2</flink.kafka.connector.version>
<jedis.version>5.2.0</jedis.version>
<postgres.driver.version>42.7.4</postgres.driver.version>
<jackson.version>2.17.2</jackson.version>
```

**Version verification (2026-07-18):**
- `redis.clients:jedis:5.2.0` — Maven Central `numFound=1` [VERIFIED: search.maven.org]
- Central latest jedis 为 6.0.0 — **不要采用**，保持仓库 SSOT 5.2.0
- 根 README 版本矩阵 **尚未列出 jedis** — Wave 0/骨架任务必须补行 [VERIFIED: rg README.md]

## Package Legitimacy Audit

> Maven 坐标；`slopcheck` 无 Maven 模式（运行时返回 unavailable）。合法性依赖 Maven Central 存在性 + 仓库已用坐标交叉验证。全部推荐包标记为已在本仓使用或 SSOT 属性区声明。

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| redis.clients:jedis:5.2.0 | Maven Central | 多年（v5.2.0 发布戳 2024-09） | 高（官方 Redis Java 客户端） | github.com/redis/jedis | N/A (Maven) | Approved — SSOT 锁定 |
| org.postgresql:postgresql:42.7.4 | Maven Central | 成熟 | 高 | github.com/pgjdbc/pgjdbc | N/A | Approved — examples/pom 已用 |
| com.fasterxml.jackson.core:jackson-databind:2.17.2 | Maven Central | 成熟 | 高 | github.com/FasterXML/jackson | N/A | Approved — p01 已用 |
| org.apache.flink:flink-* :2.2.1 | Maven Central | 锁定主线 | 高 | github.com/apache/flink | N/A | Approved — ADR-001 |
| org.apache.flink:flink-connector-kafka:5.0.0-2.2 | Maven Central | 锁定 | 高 | apache/flink-connector-kafka | N/A | Approved |

**Packages removed due to slopcheck [SLOP] verdict:** none  
**Packages flagged as suspicious [SUS]:** none  
**Note:** slopcheck Maven 不可用 → 未将坐标标 `[VERIFIED: npm registry]`；以 Maven Central + 本仓已用为准。Planner **不必**再为人造 npm 包加 checkpoint；但 **必须**在根 README 登记 jedis 后再写进 p02 pom。

## Architecture Patterns

### System Architecture Diagram

```text
                    ┌─ docker make up (Kafka/Flink/CH/PG/Redis/…) ─┐
                    │  + make up-p02 → p02-init (topics + CH DDL)  │
                    │  + seed PG reco_items (Makefile / init SQL)    │
                    └──────────────────────┬───────────────────────┘
                                           │
  gen_reco_events.py ──► Kafka reco.events │
                                           ▼
                              ┌────────────────────────┐
                              │  RealtimeRecoJob       │
                              │  ParseBehaviorJson     │
                              │  + watermark           │
                              └───────────┬────────────┘
                                          │ keyBy(userId)
                    ┌─────────────────────┼─────────────────────┐
                    ▼                     ▼                     ▼
           ┌────────────────┐   ┌─────────────────┐   ┌──────────────────┐
           │ Keyed Feature  │   │ Redis Feature   │   │ CatalogLoader    │
           │ MapState /     │──►│ Writer (jedis   │   │ PG → memory Map  │
           │ window stats   │   │ Pipeline +      │   │ (open once)      │
           │ (通道一)        │   │ CheckpointedFn) │   └────────┬─────────┘
           └───────┬────────┘   │ 可降级/不打挂   │            │
                   │            │ (通道二)         │            │
                   │            └─────────────────┘            │
                   └──────────────────┬────────────────────────┘
                                      ▼
                           ┌─────────────────────┐
                           │ RuleScorer Top-K    │
                           │ Redis 点查优先；     │
                           │ 失败 → state-only   │
                           │ reason=STATE_ONLY   │
                           └─────────┬───────────┘
                                     │
                   ┌─────────────────┴─────────────────┐
                   ▼                                   ▼
          Kafka reco.results                  ClickHouse reco_results
          （诊断）                              （verify 唯一放行）
```

### Recommended Project Structure

```text
projects/p02-realtime-reco/
├── pom.xml                          # 独立模块；jedis + postgresql + kafka
├── Makefile                         # package/submit/gen/verify/match/loadtest/drill-redis
├── README.md                        # 八段式
├── sql/
│   ├── clickhouse_reco_results.sql  # 单语句 CREATE（CH HTTP 禁多语句）
│   └── postgres_reco_items.sql      # CREATE + seed INSERT
├── scripts/
│   ├── gen_reco_events.py           # scenario + --rate/--duration
│   ├── verify.sh                    # CH 权威断言
│   ├── loadtest.sh
│   └── drill_redis_degrade.sh       # stop/pause redis → state-only → CH 仍绿
├── docs/
│   ├── ARCHITECTURE.md
│   ├── RESUME.md
│   ├── baseline.md                  # OrbStack 实测数字
│   └── adr/0001-dual-channel-features.md
├── src/main/java/com/flywhl/flinklab/p02/
│   ├── RealtimeRecoJob.java
│   ├── JobConfig.java
│   ├── ParseBehaviorJson.java
│   ├── model/{BehaviorEvent,RecoResult,ItemCatalog}.java
│   ├── feature/{SessionFeatureFunction,RedisFeatureWriter}.java
│   ├── catalog/CatalogLoader.java
│   ├── score/RuleScorer.java
│   └── sink/{KafkaRecoSink wiring, ClickHouseRecoSink}.java
└── src/test/java/.../
    ├── ParseBehaviorJsonTest.java
    ├── RuleScorerTest.java
    └── SessionFeatureFunctionTest.java   # 纯逻辑/状态辅助，避免强依赖 MiniCluster
```

### Pattern 1: 工程纪律复制（p01 骨架）

**What:** 独立 pom + `docker` profile init + Makefile 一键链 + CH verify + 2 drills + 文档包。  
**When to use:** 所有 P4 项目。  
**Example anchors:**  
- `projects/p01-log-ai-platform/`（最近样板）  
- `projects/p03-vehicle-monitoring/`（原始样板）  
- `docker/docker-compose.yml` 中 `p01-init` / `p03-init` 段落（复制为 `p02-init`）

### Pattern 2: 双通道特征（e12-06 生产化）

**What:** Keyed State 累积会话/近窗特征；并行（或下游）jedis Pipeline 攒批写 Redis；key 命名空间区分特征类型。  
**When to use:** RECO-02 强制。  
**Example:**

```java
// 教学组合见 e12-06；生产写 Redis 必须叠加 e07-C7 CheckpointedFunction
// Source: examples/e12-06-streaming-feature/.../StreamingFeatureJob.java
// Source: examples/e07-connectors/.../C7RedisBatchWriteJob.java
events.keyBy(e -> e.userId)
      .process(new SessionFeatureFunction())  // MapState → FeatureSnapshot
      .map(new RedisFeatureWriter(threshold, redisHost)) // Pipeline + ListState
      .uid("p02-redis-feature");
```

### Pattern 3: Redis 降级打分（对齐 p01 AI degrade）

**What:** Redis 写/读失败 catch + metric；打分使用随流携带的 `FeatureSnapshot`（来自 Keyed State）；`reason`/`feature_source` 标记 `STATE_ONLY`；作业不 FAIL。  
**When to use:** D-12 演练 #1。  
**对照:** `projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh`（外依赖不可达仍 verify 绿）。

### Pattern 4: ClickHouse HTTP SinkV2

**What:** 自研 SinkV2 批量 INSERT；字符串消毒；表名列名常量。  
**When to use:** 权威出口。  
**Example:** `projects/p01-log-ai-platform/.../sink/ClickHouseLogSink.java`

### Anti-Patterns to Avoid

- **e12-06 教学简化当生产：** Demo 省略 Operator State；p02 必须 CheckpointedFunction 攒批尾巴 [VERIFIED: e12-06 README]
- **Redis KEYS 放行 verify：** 违反 D-10
- **把 lettuce / jedis 6 写进 pom 未登记：** 违反 D-02 / SSOT
- **default `make up` 挂 p02-init：** 违反 D-08；对照 `docker/Makefile` 的 `up-p01`/`up-p03`
- **假装 Redis exactly-once：** D-03 明确 at-least-once；重复 SET 幂等即可
- **Grafana 大盘挡 Phase：** Discretion — MVP 可跳过专用 dashboard，用 Prom + CH 查询即可

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Redis 客户端协议 | 自写 RESP | jedis 5.2.0 | 连接/ Pipeline / 超时已成熟 |
| Kafka Source/Sink | 手工 socket | flink-connector-kafka 5.0.0-2.2 | 与 Flink 2.2 对齐 |
| CH 写入协议 | 新 JDBC 封装 | 既有 HTTP SinkV2 模式 | p01/p03 已验证；少依赖 |
| 工程脚手架 | 从零发明目录 | 复制 p01 树 | 纪律一致性 > 创意 |
| Async 维表框架 | 自研线程模型 | Flink `AsyncDataStream`（若做 Redis 异步点查）或 `open()` 加载 PG | 官方容错/水位 [CITED: nightlies flink-docs-release-2.2 asyncio] |
| 推荐算法平台 | 自研召回中台 | 规则加权 Top-K | D-04 范围锁定 |

**Key insight:** p02 的差异化在「双通道特征 + 可降级打分」叙事，不在新基础设施。

## Discretion Recommendations（Planner 可直接锁定）

| 项 | 推荐值 | 理由 |
|----|--------|------|
| CH 表 | `flinklab.reco_results` | 与 D-06 示例一致；命名对齐 `log_results` / `vehicle_alerts` |
| PG 表 | `reco_items(item_id PK, category, title, base_weight)` | 种子 50–100 行 |
| Topics | `reco.events`, `reco.results` | D-09/D-06 |
| Redis key | `feature:{userId}:click_30s`, `feature:{userId}:cat_affinity`, `feature:{userId}:last_ts` | 符合 `feature:{userId}:*`；类型分后缀 |
| eventType | `VIEW` / `CLICK` / `CART` / `BUY` | 大写枚举；造数/单测/ Sink 白名单一致 |
| 权重 | VIEW=1, CLICK=3, CART=5, BUY=10；类目匹配 ×1.5；近因半衰期 30min | 可解释、单测友好 |
| Top-K | `K=5`（`--top-k` 可覆盖） | 学习工程默认 |
| 主类 / job-name | `RealtimeRecoJob` / `p02-realtime-reco` | 目录名对齐 |
| Catalog 加载 | `CatalogLoader.load(jdbcUrl)` 于 `open()`，非每条 Async | 百级全表打分更简单；简历仍可讲「批维表 + 流特征」 |
| Redis 读写主机 | 容器内 `redis:6379`；JobConfig 可覆盖 | 对齐 e07-C7 |
| Grafana | **不做**专用 JSON（MVP） | Discretion；省一轮；监控用 Flink :9249 + CH |
| 压测起点 | RATE=100, WARMUP=30s, DURATION=90s | 对齐 p01 Discretion；executor 实测改 baseline |
| verify 断言列 | `feature_source IN ('REDIS','STATE_ONLY')` 可选；默认 `count()>=1` | 演练脚本可收紧 `STATE_ONLY` |
| p02-init 镜像 | `${KAFKA_IMAGE}`（与 p01/p03） | 不引入未登记镜像 [VERIFIED: STATE.md] |
| PG seed | `make up-p02` 内 `docker compose exec -T postgres psql … -f` | kafka 镜像无 psql；避免第二未登记镜像 |

## Common Pitfalls

### Pitfall 1: Redis 写失败打挂作业
**What goes wrong:** 演练 `docker stop fll-redis` 后 TM 持续 FAIL，CH 无行。  
**Why:** 未 catch / 未降级；或把 Redis 当 exactly-once 关键路径。  
**How to avoid:** RedisWriter try/catch；失败记 metric；FeatureSnapshot 随主流；Scorer state-only。  
**Warning signs:** Flink UI 作业 RESTARTING；drill 脚本超时。

### Pitfall 2: 只写 Redis、Keyed State 空转
**What goes wrong:** 文档宣称双通道，代码只有 Redis map。  
**Why:** 抄 e12-06 print 路径过简。  
**How to avoid:** 单测覆盖 MapState 更新；ADR 写清两通道职责；drill 证明无 Redis 仍出结果。

### Pitfall 3: CH HTTP 多语句 DDL
**What goes wrong:** init 容器 wget POST 失败。  
**Why:** CH 24.8 HTTP 禁 multiquery（p03 已踩）。  
**How to avoid:** 每文件单 `CREATE`；ALTER 拆文件。  
**Warning signs:** p02-init 非 0 exit。

### Pitfall 4: jedis 未进根 README 矩阵
**What goes wrong:** qa/评审判 SSOT 漂移。  
**How to avoid:** 骨架任务先改 README + 再写 pom。

### Pitfall 5: fll-redis / fll-postgres 未 Running
**What goes wrong:** 本机调研时两容器仅 `Created`。  
**How to avoid:** 执行前 `cd docker && make up`（或 `docker compose up -d redis postgres`）；README 写清前置。  
**Warning signs:** 连接 refused；`redis-cli ping` 失败。

### Pitfall 6: 窗口特征 key 设计错误
**What goes wrong:** e12-06 `ClickCountAgg` 教学简化未带回 userId，生产不可用。  
**How to avoid:** 用 `AggregateFunction` + `ProcessWindowFunction` 带出 key，或纯 `KeyedProcessFunction` 维护计数。

### Pitfall 7: 文档违禁词 / 断链
**What goes wrong:** `qa_check.sh` FAIL。  
**How to avoid:** 全文可执行命令；相对链接落盘后再引用。

## Code Examples

### Redis 攒批 + CheckpointedFunction（生产复刻基点）

```java
// Source: examples/e07-connectors/.../C7RedisBatchWriteJob.java
public static final class RedisBatchWriter
        extends RichMapFunction<Event, String>
        implements CheckpointedFunction {
    // buffer + ListState + jedis.pipelined().set(...).sync()
    // 语义：at-least-once；重复 SET 同 key 幂等
}
```

### Async I/O（若 Redis 点查用异步客户端 / 线程池包装）

```java
// Source: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/asyncio/
DataStream<...> result = AsyncDataStream.unorderedWait(
    stream, new RedisFeatureLookup(), 1000, TimeUnit.MILLISECONDS, 100);
// timeout 必须 ResultFuture.complete(...) —— 默认超时会重启作业
```

**p02 建议：** 打分热路径优先用随流 `FeatureSnapshot`（零外呼）；Redis 点查作为增强/对齐演示，timeout/失败 → STATE_ONLY，**禁止**默认抛异常重启。

### CH 权威 verify 骨架

```bash
# Source pattern: projects/p01-log-ai-platform/scripts/verify.sh
CH_QUERY="SELECT count() FROM flinklab.reco_results"
# Kafka / redis-cli KEYS 仅 diag_*，不得单独 exit 0
```

### Compose profile 骨架

```yaml
# Source pattern: docker/docker-compose.yml p01-init
p02-init:
  image: ${KAFKA_IMAGE}
  profiles: ["p02"]
  # 创建 reco.events / reco.results
  # wget POST clickhouse_reco_results.sql（单语句）
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| 离线日更特征 | 流式窗口 + 会话状态 → Redis | 教材 ai/06 / e12-06 | p02 项目级落地 |
| 纯 Redis demo | 双通道 + Checkpointed 攒批 + 降级 | 本 Phase（相对 e12-06） | 可讲生产语义 |
| 外部模型重排 | 规则加权 Top-K | CONTEXT D-04 | 范围可控、可断言 |

**Deprecated/outdated:**
- e12-06 省略 Operator State 的写法：教学可，p02 不可直接复制为生产路径
- lettuce 未登记路径：本 Phase 禁止

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | 百级 item 用 `open()` 全表加载优于每条 Async PG lookup | Discretion / Architecture | 若未来 catalog 很大需改 Broadcast/分片；MVP 风险低 |
| A2 | Redis 降级用 `docker stop fll-redis`（或 `compose stop redis`）可稳定复现 | Pitfalls / Validation | 若 Flink 网络缓存/重连行为异常，drill 需加等待与重提作业步骤 |
| A3 | 压测默认 100 eps / 90s 在 OrbStack arm64 可稳定出 baseline | Discretion | executor 可能下调；不挡功能验收 |
| A4 | 不做 p02 专用 Grafana JSON 仍满足 RECO-03「可观察」 | Discretion | 若用户后续要求大盘，可追加；CONTEXT 未强制 |

**已验证无需确认的锁定项：** D-01–D-13、目录名、profile 隔离、CH 权威、jedis（非 lettuce）、规则打分。

## Open Questions

1. **Redis 容器当前未 Running**
   - What we know: compose 定义了 redis/postgres；本机 `fll-redis`/`fll-postgres` 状态为 `Created`
   - What's unclear: 是否用户环境长期停用这两服务
   - Recommendation: Phase 执行 Wave 0/`up-p02` 前置显式 `make up`；drill 文档写容器名 `fll-redis`

2. **打分是否同步读 Redis 还是仅写 Redis + 用 State 打分**
   - What we know: D-01 要求 Redis「供打分阶段低延迟读取」且两者可观察
   - Recommendation: **实现「读 Redis，失败回落 State」**；正常路径 feature_source=REDIS，演练路径 STATE_ONLY——同时满足叙事与 drill

（其余决策已被 CONTEXT 锁定；无需再问用户。）

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| JDK 21 | 编译 | ✓ | 21.0.2 | — |
| Maven 3.9 | package | ✓ | 3.9.14 | — |
| Docker / OrbStack | compose | ✓ | Docker 29.4 / context orbstack | — |
| Python 3.12 + uv | 造数 | ✓ | 3.12.13 | — |
| Kafka / Flink / CH | E2E | ✓ | 运行中（调研时） | — |
| Redis (`fll-redis`) | 特征通道 / drill | ✗（Created，未 Up） | redis:7-alpine | `docker compose up -d redis`；drill 反而依赖可 stop |
| PostgreSQL (`fll-postgres`) | catalog 种子 | ✗（Created，未 Up） | postgres:16-alpine | `docker compose up -d postgres` |
| Ollama / Milvus | — | N/A | — | 本 Phase 不需要 |
| ctx7 / Context7 MCP | 文档 | ✗ | — | 已用官方 nightlies + 仓内源码 |
| slopcheck (Maven) | 包审计 | ✗ 模式不可用 | 0.6.1 已装 | Maven Central + 仓内 SSOT |

**Missing dependencies with no fallback:**
- 无阻塞性缺失工具链；**执行前必须拉起 redis/postgres**（基座服务，非新依赖）

**Missing dependencies with fallback:**
- Context7 → 官方 Flink 2.2 nightlies + 仓内 e07/e11/e12/p01

## Validation Architecture

> `workflow.nyquist_validation: true`（`.planning/config.json`）

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit Jupiter 5.10.2 + Maven Surefire 3.2.5 |
| Config file | `projects/p02-realtime-reco/pom.xml`（surefire plugin） |
| Quick run command | `cd projects/p02-realtime-reco && mvn -q -Dtest=RuleScorerTest,ParseBehaviorJsonTest test` |
| Full suite command | `cd projects/p02-realtime-reco && mvn -q test` |
| E2E gate | `cd docker && make up && make up-p02` → project `make submit && make match` |
| Phase QA | `bash scripts/qa_check.sh` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| RECO-01 | profile 创建 topic + 行为可解析 | smoke / script | `docker compose --profile p02 config -q`；`ParseBehaviorJsonTest` | ❌ Wave 0 |
| RECO-02 | 特征累积逻辑正确 | unit | `mvn -q -Dtest=SessionFeatureFunctionTest,RuleScorerTest test` | ❌ Wave 0 |
| RECO-02 | Top-K 权重/排序可判定 | unit | `RuleScorerTest` | ❌ Wave 0 |
| RECO-02 | CH 有推荐行 | e2e | `make match` → `scripts/verify.sh` | ❌ Wave 0 |
| RECO-03 | Redis 降级仍出 CH 行 | e2e drill | `scripts/drill_redis_degrade.sh` | ❌ Wave 0 |
| RECO-03 | 压测产出 baseline 段 | manual+script | `scripts/loadtest.sh` → 填 `docs/baseline.md` | ❌ Wave 0 |
| RECO-03 | ADR/ARCHITECTURE/RESUME 存在 | doc gate | `test -f docs/adr/0001-*.md` 等 | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `mvn -q -Dtest=RuleScorerTest,ParseBehaviorJsonTest test`
- **Per wave merge:** `mvn -q test` +（若环境齐）`make match`
- **Phase gate:** `make match` + `drill_redis_degrade` + `loadtest` 填 baseline + `qa_check.sh` 绿

### Wave 0 Gaps

- [ ] `projects/p02-realtime-reco/` 整树不存在 — 从 p01 复制骨架
- [ ] `ParseBehaviorJsonTest` / `RuleScorerTest` — 覆盖 RECO-02 纯逻辑
- [ ] `sql/clickhouse_reco_results.sql` + `sql/postgres_reco_items.sql`
- [ ] `docker`：`p02-init` + `Makefile` `up-p02`
- [ ] 根 README 版本矩阵登记 **jedis 5.2.0**
- [ ] `scripts/verify.sh` / `gen_reco_events.py` / `drill_redis_degrade.sh` / `loadtest.sh`
- [ ] 可选：Wave 0 RED 单测先引用尚未实现的 `RuleScorer`（对齐 p01 Wave 0 纪律）

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | 学习环境本地 compose；无用户登录面 |
| V3 Session Management | no | — |
| V4 Access Control | no | — |
| V5 Input Validation | yes | 行为 JSON 校验；CH Sink 拒引号/反斜杠；verify 白名单枚举拼 SQL（p01 T-04-01） |
| V6 Cryptography | no | 无新密钥材料；沿用 compose 默认口令（文档声明仅本地） |

### Known Threat Patterns for Flink + Redis + CH

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| CH SQL 注入（verify/脚本） | Tampering | 枚举白名单；禁止未校验用户串进查询 |
| Sink 拼接 INSERT | Tampering | 字段消毒 + 常量表名列名（ClickHouseLogSink 模式） |
| Redis 命令注入（key） | Tampering | key 仅由 `userId` 校验字符集拼接；禁止把原始 JSON 当 key |
| 凭证进仓 | Information Disclosure | 沿用 compose 默认；勿新增真实云密钥 |
| 作业被恶意参数打爆 | DoS | JobConfig 上限（top-k、batch size、rate 脚本 MAX_*） |

## Canonical File Paths

| Role | Path |
|------|------|
| 本 Phase 产出根 | `projects/p02-realtime-reco/` |
| Compose 扩展 | `docker/docker-compose.yml`（`p02-init`） |
| Make 入口 | `docker/Makefile`（`up-p02`）+ 项目 `Makefile` |
| 特征教材 | `ai/chapters/06-streaming-feature.md` |
| 特征 Demo | `examples/e12-06-streaming-feature/` |
| Redis 生产写 | `examples/e07-connectors/.../C7RedisBatchWriteJob.java` |
| Async/维表 | `examples/e11-async-io/` |
| 工程样板 | `projects/p01-log-ai-platform/`、`projects/p03-vehicle-monitoring/` |
| 文档编号 | `docs/README.md` §15-02 |
| 版本 SSOT | `README.md`、`examples/pom.xml` |
| QA 门禁 | `scripts/qa_check.sh` |
| CONTEXT | `.planning/phases/05-p02/05-CONTEXT.md` |

## Sources

### Primary (HIGH confidence)
- `.planning/phases/05-p02/05-CONTEXT.md` — 锁定决策 D-01–D-13
- `README.md` / `examples/pom.xml` — 版本 SSOT；jedis 5.2.0
- `projects/p01-log-ai-platform/` — verify/drill/pom/Makefile/Sink 样板
- `projects/p03-vehicle-monitoring/` — profile/init/baseline/ADR 样板
- `examples/e12-06-streaming-feature/`、`examples/e07-connectors/C7RedisBatchWriteJob.java`
- `docker/docker-compose.yml` — p01/p03 init 可复制段落
- [Flink 2.2 Async I/O](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/asyncio/) — timeout/complete 语义 [CITED]
- Maven Central — `redis.clients:jedis:5.2.0` 存在性 [VERIFIED]

### Secondary (MEDIUM confidence)
- `.planning/research/{STACK,ARCHITECTURE,FEATURES,PITFALLS}.md` — p02 示意与纪律陷阱
- `ai/chapters/06-streaming-feature.md` — 双通道论述与踩坑表

### Tertiary (LOW confidence)
- 压测默认 eps/时长在本机的最终数字 — 待 OrbStack 实测（A3）

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — 全仓已锁定版本 + Central 核验 jedis
- Architecture: HIGH — 样板齐全；域路径与 CONTEXT 一致
- Pitfalls: HIGH — 来自 p01/p03/e12 已知坑 + Redis 降级设计点

**Research date:** 2026-07-18  
**Valid until:** 2026-08-17（30 days；Flink/连接器 SSOT 稳定）

## Findings（planner 速览）

1. **复制 p01 工程壳，填推荐域内核** — 最快满足 RECO-01/03。
2. **双通道是验收叙事核心** — State 必做且支撑降级；Redis 必写且 at-least-once。
3. **CH `reco_results` 唯一放行** — Kafka/Redis 只诊断。
4. **先登记 jedis 到根 README** — 否则 SSOT 违规。
5. **执行前拉起 redis/postgres** — 当前容器可能未 Running。

## Recommendations

1. Plan 切 3–4 个 wave：骨架+SSOT → 特征/Redis → 打分/双写/verify → 压测+drill+文档包。
2. Wave 0 建立 RED 单测（RuleScorer/Parse）与空脚本 `--implemented` 门闩（可选，对齐 p01）。
3. ADR-0001 标题固定双通道取舍；演练脚本断言 `STATE_ONLY` 或等价列。
4. Catalog 用 PG seed + `open()` 加载；勿引入 Milvus/lettuce/新镜像。

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Redis 降级设计不当导致作业挂 | 挡 RECO-03 | 写路径 catch；读路径回落；drill 自动化 |
| SSOT 漏登记 jedis | 评审/门禁 | README 矩阵先行 |
| e12-06 简化代码被原样粘贴 | 丢容错 | 强制引用 e07-C7 CheckpointedFunction |
| PG/Redis 未 up | E2E 假失败 | README/Makefile 前置检查 |

## RESEARCH COMPLETE
