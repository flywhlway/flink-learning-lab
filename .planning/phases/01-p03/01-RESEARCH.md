# Phase 1: p03 告警链路样板 - Research

**Researched:** 2026-07-17
**Domain:** Apache Flink CEP 车联网告警链路 + Docker Compose profile 隔离 + 端到端验证断言
**Confidence:** HIGH

## Summary

本 Phase 是 P4 三大生产项目的**工程样板起点**：在已交付的 `docker/` 基座与 `examples/e10-cep`（尤其 C5 车联网雏形、C3 超时 Side Output）之上，新建 `projects/p03-vehicle-monitoring`，用独立 compose profile `p03` 一键补齐 topic/表结构，交付**单模式** CEP 告警作业，并用带断言的验证脚本证明「事件→Kafka→CEP→Side Output→ClickHouse/Kafka 告警」可复现。Broadcast 动态模式、≥3 模式五元组、Grafana 大盘与压测演练明确归属 Phase 2/3，本 Phase 不做。

仓库当前**尚无** `projects/` 目录与 `p03` profile；`make up` / `profiles: ["ai"]` 已是可复制的隔离样板。验证侧几乎无自动化测试基础设施（examples 无 `src/test`），必须以项目内 `scripts/verify.sh`（失败非 0 + 可观察断言）作为 Nyquist 主路径，并可选补一条 MiniCluster/JUnit 模式单测作为 Wave 0。

**Primary recommendation:** 复制 `templates/job-datastream` → `projects/p03-vehicle-monitoring`；compose 按 ai-profile 同构增加 `profiles: ["p03"]` 的一次性 init 服务（topic + CH 表）；作业复用 e10-C5 `followedBy`+`within` 并接入 e10-C3 式 Side Output；告警双写 Kafka + ClickHouse HTTP SinkV2（对齐 e07-C6）；造数注入可判定序列；`verify` 对 CH/Kafka 计数断言。

<user_constraints>
## User Constraints（无 CONTEXT.md — discuss-phase 已跳过；以下为 PROJECT / ROADMAP / REQUIREMENTS 锁定口径）

### Locked Decisions
- 里程碑覆盖 P4+P5+P6；顺序 p03→p01→p02→P5→P6
- P4 验收三项全硬；**p03 先告警后大盘**
- GSD 按交付物切细（7 phases）；Vertical MVP
- 主线锁定 Flink 2.2.1（ADR-001）；不升 2.3
- Phase 1 仅 VEH-01、VEH-02；Broadcast / 模式库五元组 ≥3 / Grafana / 压测演练 / 完整文档包属后续 Phase
- 一切须在 OrbStack arm64 实测；不可验证不合入；禁止 TODO / 省略 / 略 / 自行实现 / 请参考官网
- 版本 SSOT：根 README 版本矩阵 + pom 属性区；文档编号先在 `docs/README.md` 登记；八段式
- 目标目录名：`projects/p03-vehicle-monitoring`（docs/README 模块 15 已登记）

### Claude's Discretion（无 CONTEXT；研究员推荐如下，planner 可采纳）
- compose 实现形态：同文件 `profiles: ["p03"]` 一次性 init（对齐现有 ai profile），而非独立 overlay 文件
- MVP 告警观测：匹配结果双写 `vehicle.alerts`（Kafka）+ ClickHouse `vehicle_alerts`；超时半成品走 Side Output 至少一路可观测
- 造数：项目内专用生成器（可判定 harsh→fault 序列），不复用通用 `gen_events.py` 的点击页语义
- Maven：`projects/p03-vehicle-monitoring` 独立 pom（对齐 templates），不挂入 `examples/` 父工程
- 本 Phase 文档：项目 README 八段式最小集即可；完整 ADR/简历页可延至 Phase 3（VEH-07），但须在 docs/README 登记编号占位

### Deferred Ideas (OUT OF SCOPE)
- VEH-03/04（模式库 ≥3 + Broadcast 动态选择）→ Phase 2
- VEH-05/06/07（Grafana 大盘、压测/watermark 演练、完整 ADR/简历页）→ Phase 3
- 商业动态 CEP 引擎、云托管作为唯一演示路径、真实车企数据
- Flink 2.3 主线、StateFun
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| VEH-01 | 维护者可用独立 compose profile 一键启动 p03，且不影响 default `make up` | Compose Profile Isolation 模式；`profiles: ["p03"]` init 服务；`make up` 不加 `--profile p03`；`docker compose config` 门禁 |
| VEH-02 | 维护者可复现告警链路（事件→Kafka→CEP→Side Output→ClickHouse/通知），验证脚本对可观察输出做断言 | e10-C5/C5 模式 + e10-C3 Side Output；e01 KafkaSource/Sink；e07-C6 CH SinkV2；项目 `verify.sh` 断言样板 |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Compose profile / topic·表初始化 | CDN / Static（本地编排层：Docker Compose） | Database / Storage | profile 只追加运维资源，不进业务逻辑 |
| 车端事件造数 | API / Backend（宿主机脚本） | — | 合成数据注入 Kafka EXTERNAL listener |
| 事件总线 | Database / Storage（Kafka） | — | 已有基座服务，p03 只增 topic |
| CEP 匹配 + Side Output | API / Backend（Flink Job） | — | DataStream + flink-cep 拥有状态与时间语义 |
| 告警落库 / 通知通道 | Database / Storage（ClickHouse + Kafka alerts） | — | 可观察输出落在 CH 行数 / topic 消息 |
| 验证断言 | API / Backend（bash/Python verify） | — | 对可观察产物做非 0 退出断言 |
| Flink JM/TM / 默认中间件生命周期 | CDN / Static（docker 基座） | — | default `make up` 必须保持可用 |

## Project Constraints (from .cursor/rules/)

未发现 `.cursor/rules/` 目录。本阶段以仓库根 `CLAUDE.md` / `PHASES.md` / 根 README 工程约定为准（版本 SSOT、docs 编号登记、OrbStack 实测、违禁词、`qa_check.sh`）。

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Apache Flink | 2.2.1 | 作业运行时 | 根 README SSOT；镜像 `flink:2.2.1-java21` [VERIFIED: examples/pom.xml + README] |
| JDK | 21 | 编译与运行 | 本机 `openjdk 21.0.2`；模板 `maven.compiler.release=21` [VERIFIED: env + templates] |
| flink-cep | 2.2.1 | Pattern API / NFA | 随 Flink 同版本；e10 已用 [VERIFIED: examples/pom.xml + e10] |
| flink-connector-kafka | 5.0.0-2.2 | Kafka Source/Sink | 官方兼容 2.2.x [VERIFIED: examples/pom.xml] |
| Kafka | apache/kafka:3.9.1 KRaft | 事件总线 | docker `.env` 已锁定 [VERIFIED: docker/.env] |
| ClickHouse | clickhouse/clickhouse-server:24.8 | 告警落库 | 基座已有；HTTP 8123 [VERIFIED: docker/.env] |
| Docker Compose profiles | Compose v5.x | p03 隔离启动 | 现有 `profiles: ["ai"]` 样板 [VERIFIED: docker-compose.yml] |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| ClickHouse HTTP SinkV2 | 手写（对齐 e07-C6） | 告警批写 CH | Phase 1 落库；勿引入未登记商业连接器 [CITED: e07-C6] |
| Jackson / SimpleStringSchema | 随 Flink / 现有用法 | JSON 解析 | 对齐 e01 ParseJson [CITED: e01] |
| confluent-kafka (Python) | ≥2.5（脚本声明） | 造数 Producer | 项目 `gen_vehicle_events.py`；仓库已有 `scripts/gen_events.py` 范式 [VERIFIED: scripts/gen_events.py]；slopcheck [OK] |
| JUnit Jupiter | 5.10.2 | 可选模式单测 | templates 已声明；Wave 0 可选 [VERIFIED: templates/job-datastream/pom.xml] |
| flink-test-utils | 2.2.1 | MiniCluster 单测 | 模板已声明 test scope [VERIFIED: templates] |
| uv | 本机 0.10.10 | 运行 Python 脚本 | 与 gen_events 一致 [VERIFIED: env] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| 同文件 `profiles: ["p03"]` | `docker-compose.p03.yml` overlay | overlay 更清晰但偏离现有 ai 样板；本 Phase 优先同构 |
| HTTP SinkV2（e07-C6） | JDBC Sink / 官方 CH 连接器 | 新坐标需先登记 SSOT；HTTP 已有可复制实现 |
| 仅 CH 落库 | 仅 Kafka alerts | 双写便于「Kafka 通道 + CH 查询」双断言，满足 VEH-02 措辞 |
| 确定性造数序列 | 纯随机 eps 流 | 随机难稳定断言；MVP 必须可判定注入 |
| 本 Phase 做 Broadcast | 单硬编码 Pattern | Broadcast 属 Phase 2（VEH-04）；提前做会膨胀范围 |

**Installation:**

```bash
# 基座（default，不含 p03）
cd docker && make up && make init

# p03 profile（规划实现后）
docker compose --profile p03 up -d
# 或预留: make up-p03

# 作业
cd projects/p03-vehicle-monitoring && mvn -q clean package
# 提交 jar → docker/jobs + flink run（对齐 docker/Makefile submit-e01）
```

**Version verification:**
- `flink.version=2.2.1`、`flink.kafka.connector.version=5.0.0-2.2` — `examples/pom.xml` / `templates/job-datastream/pom.xml` [VERIFIED]
- ClickHouse / Kafka 镜像 — `docker/.env` [VERIFIED]
- 官方 CEP `Pattern.within(Duration)` + `TimedOutPartialMatchHandler` — Flink 2.2 Javadoc / CEP docs [CITED: nightlies.apache.org/flink/flink-docs-release-2.2]

## Package Legitimacy Audit

> Phase 主要复用已锁定的 Apache Maven 坐标；Python 造数依赖 `confluent-kafka`（仓库既有）。未引入新的 npm 包。

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| org.apache.flink:flink-cep:2.2.1 | Maven Central | 随 Flink 发行 | N/A（Apache） | github.com/apache/flink | N/A（非 PyPI） | Approved — SSOT [VERIFIED: pom] |
| org.apache.flink:flink-connector-kafka:5.0.0-2.2 | Maven Central | 官方连接器线 | N/A | github.com/apache/flink-connector-kafka | N/A | Approved — SSOT [VERIFIED: pom] |
| confluent-kafka | PyPI | 多年 | 高 | github.com/confluentinc/confluent-kafka-python | [OK] | Approved（造数脚本） |
| junit-jupiter:5.10.2 | Maven Central | 稳定 | N/A | junit-team | N/A | Approved — 模板已用 |

**Packages removed due to slopcheck [SLOP] verdict:** none  
**Packages flagged as suspicious [SUS]:** none  

*Maven 坐标未走 slopcheck（工具面向 PyPI/npm）；合法性依据为 Apache 官方发行 + 仓库 SSOT。*

## Architecture Patterns

### System Architecture Diagram

```text
[gen_vehicle_events.py] --produce--> Kafka topic vehicle.events
                                            |
                                     Flink JobManager/TM
                                            |
                    KafkaSource → parse → watermark → keyBy(vin)
                                            |
                              CEP Pattern (e10-C5 同构)
                     harsh ──followedBy──► fault  .within(30s)
                           /                      \
                    processMatch              processTimedOutMatch
                           |                      |
                    main: AlertEvent         Side Output: TimeoutAlert
                           |                      |
              ┌────────────┴──────────┐           |
              v                       v           v
     Kafka vehicle.alerts    ClickHouse      (可合并写 alerts
                              vehicle_alerts   或单独 tag 观测)
              ^
              |
     [verify.sh] 断言: topic 有消息 AND/OR CH count() >= N
                   失败 → exit ≠ 0

Compose:
  default make up  → kafka/flink/ch/...（不变）
  --profile p03    → p03-init（创建 topic + CH DDL）一次性完成
```

### Recommended Project Structure

```text
projects/p03-vehicle-monitoring/
├── pom.xml                          # 独立模块，版本对齐 SSOT
├── README.md                        # 八段式：背景→架构→代码→启动→验证→踩坑→实践→参考
├── Makefile                         # up/package/submit/verify 入口
├── sql/
│   └── clickhouse_alerts.sql        # vehicle_alerts DDL（亦由 p03-init 执行）
├── scripts/
│   ├── gen_vehicle_events.py        # 可判定造数（含 harsh→fault 注入）
│   └── verify.sh                    # 断言 + set -euo pipefail；失败非 0
├── src/main/java/.../p03/
│   ├── VehicleAlertJob.java         # main 装配（对齐 JobTemplate）
│   ├── JobConfig.java
│   ├── model/VehicleEvent.java      # vin, signalType, value, eventTime
│   ├── model/AlertEvent.java
│   ├── cep/HarshThenFaultPattern.java
│   ├── cep/AlertPatternHandler.java # PatternProcessFunction + TimedOutPartialMatchHandler
│   └── sink/ClickHouseAlertSink.java# 复用 e07-C6 模式，改表/字段
└── src/test/java/.../               # Wave 0：模式条件单测（可选但推荐）
```

Docker 侧增量（建议）：

```text
docker/docker-compose.yml
  └── service p03-init:
        profiles: ["p03"]
        # 幂等创建 vehicle.events / vehicle.alerts + 执行 CH DDL
docker/Makefile
  └── up-p03: docker compose --profile p03 up -d ...
```

### Pattern 1: Compose Profile Isolation（对齐 ai profile）
**What:** 仅把 p03 **专用**资源标 `profiles: ["p03"]`；default `docker compose up` / `make up` 不启动它们。  
**When to use:** 任何生产项目一键起（VEH-01 / LOG-01 / RECO-01）。  
**Example:** 见 `docker/docker-compose.yml` 中 `milvus-*` 的 `profiles: ["ai"]` [VERIFIED: codebase]。

### Pattern 2: CEP 匹配 + 超时 Side Output
**What:** `Pattern.begin.followedBy.within` + `PatternProcessFunction` mixin `TimedOutPartialMatchHandler`，超时经 `OutputTag` Side Output。  
**When to use:** 告警链路需要「发生了」与「该发生却未发生」双通道（VEH-02）。  
**Example:**

```java
// Source: nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/
// 仓库对照: examples/e10-cep/.../C3TimeoutSideOutputJob.java + C5VehicleDtcPatternJob.java
Pattern<VehicleEvent, ?> pattern = Pattern.<VehicleEvent>begin("harsh")
    .where(SimpleCondition.of(e -> "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
    .followedBy("fault")
    .where(SimpleCondition.of(e -> "DTC".equals(e.signalType) && e.value > 480))
    .within(Duration.ofSeconds(30));

SingleOutputStreamOperator<AlertEvent> matched =
    CEP.pattern(keyed, pattern).process(new AlertPatternHandler(timeoutTag));
matched.getSideOutput(timeoutTag); // 超时旁路
```

### Pattern 3: 可观察断言验证（非 echo）
**What:** `verify.sh` 查询 CH `count()` / 消费 Kafka 条数 / Flink REST job 状态，不达标则 `exit 1`。  
**When to use:** 每个生产项目硬验收（Core Value）。  
**Anti-echo:** `.planning/research/PITFALLS.md` 明确禁止仅 echo 的假完成 [CITED: PITFALLS]。

### Anti-Patterns to Avoid
- **在 default 路径硬依赖 p03 topic/表：** 破坏 `make up`（Pitfall 2）
- **无 `within` 的 CEP：** 状态无 TTL，禁止合入（docs/10-cep 红线）
- **把 e10 Demo 膨胀成生产项目：** 教学保持纯度；逻辑复制到 `projects/`
- **验证脚本只打印成功：** 违反 VEH-02 / Success Criteria 3
- **本 Phase 实现 Broadcast / Grafana / 压测：** 范围污染 Phase 2/3
- **CH Sink 字符串拼接用户输入且不校验：** 见 Security Domain

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CEP NFA / within / 超时 | 自写状态机 + Timer | flink-cep Pattern API | 部分匹配、watermark 联动、skip 策略已处理 [CITED: Flink CEP docs] |
| Kafka 消费位点 / 序列化 | 自研客户端环 | flink-connector-kafka Source/Sink | 与 checkpoint 语义集成 [CITED: e01] |
| Compose 服务隔离 | 改 default 服务列表塞业务 | `profiles: ["p03"]` | 已有 ai 样板；保护 `make up` |
| CH 批写时机 | 自写 CheckpointedFunction 攒批（除非教学） | SinkV2 `flush`（e07-C6） | checkpoint 前强制 flush 接口已标准化 |
| JSON Schema 演化框架 | 自建 Registry | Phase 1 固定 POJO 字段 | MVP 范围；演进留后续 |

**Key insight:** 本 Phase 价值在「工程纪律样板」（profile + 断言 + 可复现链路），不在发明新 CEP 算法。

## Common Pitfalls

### Pitfall 1: 破坏 default `make up`
**What goes wrong:** 无 profile 的服务依赖 p03 topic 或不存在的镜像，主干无法起。  
**Why it happens:** 把项目初始化写进 `init.sh` 默认路径。  
**How to avoid:** p03 DDL/topic 只进 `profiles: ["p03"]`；PR 跑 `docker compose config`（qa_check 已覆盖 default）。  
**Warning signs:** `make up` 失败；端口冲突文档。

### Pitfall 2: 验证脚本假完成
**What goes wrong:** `echo OK; exit 0`，无 CH/Kafka 断言。  
**Why it happens:** 时间紧、沙箱未跑。  
**How to avoid:** `verify.sh` 必须 `count >= N` 或消息存在检测；失败非 0。  
**Warning signs:** 脚本无 `clickhouse-client` / `kafka-console-consumer` / curl 检查。

### Pitfall 3: Watermark 停滞导致 CEP「无告警」
**What goes wrong:** 造数乱序过大、无 idleness、或只用处理时间。  
**Why it happens:** 混淆事件时间与处理时间；低流量分区拖死 watermark。  
**How to avoid:** 事件带 `eventTime`；`forBoundedOutOfOrderness` + `withIdleness`（对齐 e01）；确定性注入保证 within 窗口内有序对。  
**Warning signs:** Flink UI 有数据但无 match；超时从不触发。

### Pitfall 4: 宿主机 vs 容器 bootstrap 混用
**What goes wrong:** 作业用 `localhost:9094` 在容器内失败，或造数用 `kafka:9092` 在宿主机失败。  
**Why it happens:** 双 listener（INTERNAL/EXTERNAL）。  
**How to avoid:** 造数默认 `localhost:9094`；Flink 作业默认 `kafka:9092`（JobConfig 可覆盖）。  
**Warning signs:** Connection refused；与 `scripts/gen_events.py` 注释矛盾。

### Pitfall 5: CEP 无 within / 宽松连接爆炸
**What goes wrong:** TM 内存爬升；匹配暴增。  
**Why it happens:** 复制模式时漏掉 within 或误用 `followedByAny`。  
**How to avoid:** MVP 固定 `followedBy` + `within(30s)`；文档注明连接语义（完整五元组 Phase 2 强制）。  
**Warning signs:** 状态大小持续涨。

### Pitfall 6: e07-C6 flush 日志 bug 原样复制
**What goes wrong:** `buffer.clear()` 后再打印 `buffer.size()` 恒为 0，误导排障。  
**Why it happens:** e07-C6 示例打印顺序错误 [VERIFIED: C6ClickHouseHttpSinkJob.java L90-92]。  
**How to avoid:** p03 Sink 先记录 size 再 clear；并检查 HTTP 非 2xx 抛错（示例仅注释提醒）。

## Code Examples

### Kafka Source + 事件时间（对齐 e01）

```java
// Source: examples/e01-hello-flink/.../KafkaClickstreamWindowJob.java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers(bootstrap) // 容器内 kafka:9092
    .setTopics("vehicle.events")
    .setGroupId("p03-vehicle-alerts")
    .setStartingOffsets(OffsetsInitializer.earliest()) // 验证友好；生产可 latest
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<VehicleEvent> events = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "vehicle-events")
    .uid("p03-source")
    .map(new ParseVehicleJson())
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((e, ts) -> e.eventTime)
            .withIdleness(Duration.ofSeconds(30)));
```

### verify.sh 断言骨架

```bash
#!/usr/bin/env bash
set -euo pipefail
# 失败必须非 0（VEH-02 / Success Criteria 3）
COUNT=$(docker compose -f docker/docker-compose.yml exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "SELECT count() FROM flinklab.vehicle_alerts")
if [[ "${COUNT}" -lt 1 ]]; then
  echo "FAIL: expected vehicle_alerts rows >= 1, got ${COUNT}" >&2
  exit 1
fi
echo "ok alerts_rows=${COUNT}"
```

### p03-init 幂等 topic（对齐 init.sh）

```bash
# Source pattern: docker/init.sh create_topic
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vehicle.events --partitions 4 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vehicle.alerts --partitions 4 --replication-factor 1
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| e10-C5 本地 Datagen + print | Kafka 入 / Kafka+CH 出的生产骨架 | P4 Phase 1 | 可复现硬验收 |
| 默认全量 compose 塞业务 | Compose profiles 隔离（ai 已示范） | P3 起 | 保护 `make up` |
| Pattern 超时丢弃 | `TimedOutPartialMatchHandler` + Side Output | Flink CEP 长期能力；2.2 文档仍推荐 | 「没发生」可变告警 |

**Deprecated/outdated:**
- 在学习工程内引入商业动态 CEP 作为 Phase 1 路径 — 拒绝（STACK / FUT-02）
- 无断言的「文档验收」— 本里程碑 Core Value 禁止

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | p03 profile 仅需一次性 init（topic+DDL），无需新增长期运行容器 | Architecture | 若需独立通知 Webhook 服务，需增 profile 服务与端口规划 |
| A2 | 告警双写 Kafka+CH 为 MVP 最优观测组合 | Discretion | 用户若只要单通道，可删一路但仍须保留可断言出口 |
| A3 | Phase 1 可不交付完整 ADR/简历页（VEH-07 在 Phase 3） | Discretion | 若执行期要求「每 Phase 都有 ADR」，需补最小 ADR |
| A4 | 确定性造数比随机流更适合断言 | Pitfalls | 若只做随机流，verify 需更长等待与统计阈值，易 flaky |

**说明:** A1–A4 为范围/产品选择假设（非未经核实的库名）。技术栈坐标均已 VERIFIED/CITED。

## Open Questions

1. **p03-init 实现载体**
   - What we know: ai profile 用长期服务；init.sh 用宿主机 bash。
   - What's unclear: 用 `profile` 挂载的 one-shot container，还是 `make up-p03` 调项目脚本。
   - Recommendation: one-shot compose service + Makefile 封装，保证「一键」与文档路径一致。

2. **Flink 作业提交方式**
   - What we know: `submit-e01` 复制 jar 到 `docker/jobs` 再 `flink run`。
   - What's unclear: p03 是否提供同等 Makefile 目标还是项目内 `submit.sh`。
   - Recommendation: 项目 `Makefile` 提供 `submit`/`verify`，docker Makefile 可加薄封装 `submit-p03`。

3. **超时 Side Output 是否写入同一 CH 表**
   - What we know: VEH-02 要求 Side Output 在链路中。
   - What's unclear: 超时与匹配是否同表加 `alert_type` 字段。
   - Recommendation: 同表 + `alert_type ENUM('MATCH','TIMEOUT')`，verify 至少断言 MATCH≥1（TIMEOUT 可选注入场景）。

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker / Compose | VEH-01 | ✓ | Docker 29.4.0 / Compose v5.1.2；Context=orbstack | — |
| OrbStack arm64 | 实测验收 | ✓ | 宿主机 `arm64`；容器 `aarch64` | — |
| JDK 21 | 编译作业 | ✓ | openjdk 21.0.2 | — |
| Maven 3.9+ | package | ✓ | 3.9.14 | — |
| uv | 造数脚本 | ✓ | 0.10.10 | `python3` + venv |
| Kafka/Flink/CH 基座 | 端到端 | ✓（需 `make up`） | 版本见 SSOT | — |
| Context7 MCP | 文档检索 | ✗（server error） | — | 官方 nightlies + 仓库 e10/e07 |
| 知识图谱 graph.json | 交叉引用 | ✗ | — | 以 docs/PHASES/research 为准 |

**Missing dependencies with no fallback:**
- 无阻塞项（本机工具链齐全）。端到端仍依赖维护者执行 `cd docker && make up`。

**Missing dependencies with fallback:**
- Context7 → 官方 Flink 2.2 CEP 文档 + 仓库已验证 Demo

## Validation Architecture

> `workflow.nyquist_validation: true` — 本节供后续 VALIDATION.md 生成。

### Test Framework

| Property | Value |
|----------|-------|
| Framework | 主路径：bash 断言脚本；辅：JUnit Jupiter 5.10.2 + flink-test-utils 2.2.1（模板已声明） |
| Config file | 无仓库级 surefire 聚合 — Wave 0 在 p03 pom 启用 `maven-surefire-plugin` |
| Quick run command | `cd projects/p03-vehicle-monitoring && mvn -q -Dtest=HarshThenFaultPatternTest test`（Wave 0 后） |
| Full suite command | `projects/p03-vehicle-monitoring/scripts/verify.sh`（依赖 OrbStack 集群已 up + 作业已跑 + 造数） |
| 仓库门禁 | `bash scripts/qa_check.sh`（compose config / 违禁词 / 断链 / 案例计数 / mvn examples） |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VEH-01 | `--profile p03` 可启动；default `make up` 不拉 p03-init | smoke | `cd docker && docker compose config -q` && `docker compose --profile p03 config -q`；人工/脚本确认 `make up` 无 p03 容器 | ❌ Wave 0 |
| VEH-01 | p03 topic/表被创建 | smoke | `verify` 前置或 init 后 `kafka-topics --list` 含 `vehicle.events` | ❌ Wave 0 |
| VEH-02 | CEP 匹配告警可观察 | e2e | `scripts/verify.sh` 断言 CH `count()>=1` 或 alerts topic 有消息 | ❌ Wave 0 |
| VEH-02 | Side Output 在拓扑中存在 | unit / e2e | JUnit：Handler 对超时调用 `ctx.output`；或 e2e 注入半成品后断言 TIMEOUT 行 | ❌ Wave 0 |
| VEH-02 | 验证失败非 0 | smoke | 故意空库跑 `verify.sh` 期望 exit 1 | ❌ Wave 0 |
| ENG-01 | 无新版本漂移 | gate | `qa_check.sh` + 人工核对 README 矩阵 | ✅ scripts/qa_check.sh |

### Sampling Rate
- **Per task commit:** `mvn -q -pl` 等效的 p03 `mvn -q test`（若有单测）+ `docker compose config -q`
- **Per wave merge:** `scripts/verify.sh` 在 OrbStack 上跑通
- **Phase gate:** `verify.sh` 绿 + `scripts/qa_check.sh` 绿 + default `make up` 冒烟

### Wave 0 Gaps
- [ ] `projects/p03-vehicle-monitoring/scripts/verify.sh` — 覆盖 VEH-02 断言与失败退出码
- [ ] `projects/p03-vehicle-monitoring/src/test/java/.../HarshThenFaultPatternTest.java` — 覆盖模式条件 / within 语义（不替代 OrbStack e2e）
- [ ] `docker compose --profile p03 config` 纳入文档/脚本检查（qa_check 目前只验 default config）
- [ ] Framework：p03 pom 启用 surefire（模板未配 plugin 时需补）

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no（本地学习 lab，无对外 IdP） | — |
| V3 Session Management | no | — |
| V4 Access Control | partial | Compose 网络默认本地；勿对公网暴露 8123/9094 而不加防火墙 |
| V5 Input Validation | yes | JSON 解析失败 → Side Output/丢弃脏数据；字段白名单（signalType 枚举） |
| V6 Cryptography | no（明文 PLAINTEXT lab） | 文档标明非生产安全基线 |

### Known Threat Patterns for Flink + Kafka + ClickHouse lab

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| CH HTTP INSERT 字符串拼接注入 | Tampering | 校验/转义字段；或改用参数化/Values 预校验；拒绝含引号的 vin |
| 恶意超大 JSON / 嵌套 | Denial of Service | 限制报文大小；解析失败计数指标 |
| 脏事件拖垮 CEP 状态 | Denial of Service | 强制 within；过滤非法 signalType |
| 凭证进 git | Information Disclosure | 沿用 docker/.env 本地约定；不新增真实密钥 |
| 作业 jar 路径遍历提交 | Tampering | Makefile 固定 artifact 名；不接受任意用户路径 |

## Sources

### Primary (HIGH confidence)
- 仓库：`examples/e10-cep`（C5/C3）、`examples/e07-connectors`（C6）、`examples/e01-hello-flink`（Kafka IO）、`docker/docker-compose.yml`（ai profile）、`templates/job-datastream`
- 仓库规划：`.planning/research/{ARCHITECTURE,PITFALLS,STACK}.md`、`PROJECT.md`、`REQUIREMENTS.md`、`ROADMAP.md`
- [CITED: https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/] — PatternProcessFunction、TimedOutPartialMatchHandler、within
- [CITED: https://nightlies.apache.org/flink/flink-docs-release-2.2/api/java/org/apache/flink/cep/functions/TimedOutPartialMatchHandler.html]

### Secondary (MEDIUM confidence)
- `.planning/research/FEATURES.md` — p03 与 e10 C5 追溯关系
- Compose profile 行为 — 以现有 ai profile 实测结构为准（本机 `docker compose config` default 已通过）

### Tertiary (LOW confidence)
- 通知 Webhook 形态（可选旁路）— 本 Phase 非必须；未做协议选型

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — SSOT + 本机环境 + 官方 CEP 文档交叉验证
- Architecture: HIGH — 与 ARCHITECTURE.md / e10 / ai-profile 一致；projects/ 尚未创建属预期缺口
- Pitfalls: HIGH — 来自仓库 PITFALLS + docs/10-cep 红线 + 本机 bootstrap 语义

**Research date:** 2026-07-17  
**Valid until:** 2026-08-16（Flink 2.2.1 稳定线；若升级连接器版本需重验）
