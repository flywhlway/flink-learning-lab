# Phase 4: p01 日志 AI 平台 - Research

**Researched:** 2026-07-18
**Domain:** Flink DataStream Async I/O · 宿主机 Ollama HTTP · 可降级日志富化/规则 · ClickHouse 权威验收 · p03 工程纪律复制
**Confidence:** HIGH（工程骨架/验收纪律有对齐 p03 的实测样板；AI 路径机制有 Flink 2.2 Async I/O 官方文档 + 本机 Ollama `/api/chat` 实测；Agents/RAG 按 CONTEXT 降为非硬依赖）

## Summary

Phase 4 要把 `projects/p01-log-ai-platform` 做到与 p03 同等的单项目完成态：独立 `--profile p01` 一键起、**默认零 AI 依赖**的结构化日志解析→富化/特征→规则路径（CH 权威断言）、可选 **Async I/O → Ollama `/api/chat` 风险分级**旁路（`verify-ai`）、护栏+预算熔断的 Flink 自定义指标可观察，以及项目级压测/2 条演练/ADR/ARCHITECTURE/RESUME。CONTEXT D-01–D-15 已锁死主路径选型与验收双轨；研究焦点是 **如何按 p03 纪律落地这些决策**，而不是重开 ML_PREDICT / Agents / Milvus 选型。

本机 OrbStack 已验证：JobManager/TaskManager 均可经 `http://host.docker.internal:11434` 访问宿主机 Ollama；`POST /api/chat` + `"stream":false` + `"format":"json"` + `"think":false` 可返回可解析的 `{"risk":"..."}`。当前宿主机已装模型为 `qwen3.5:9b-mlx` / `qwen3.6:35b-a3b`，**不等于** README SSOT 建议的 `qwen3:8b`——JobConfig 必须可覆写模型名，文档写清 `ollama pull` 或 `--ai.model` 覆盖。

**Primary recommendation:** 垂直切片复制 p03（独立 pom + `p01-init` + CH HTTP SinkV2 + `verify`/`loadtest`/`drill` + 八段式文档），主作业默认 `--ai.enabled=false` 只跑规则/特征落 `flinklab.log_results`；开启 AI 时用 `AsyncDataStream.unorderedWaitWithRetry` + 线程池包装的 Ollama HTTP（`stream:false`、超时走 `timeout()` 降级到规则），护栏/预算用 Broadcast 或静态配置 + Flink `MetricGroup` Counter；门禁以 `verify` 为准，`verify-ai` 本机可选。

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** 主 AI 路径锁定为 **DataStream Async I/O → 宿主机 Ollama HTTP（`/api/chat`）做日志风险分级**（HIGH/MEDIUM/LOW 或等价枚举）。对齐 e11 Async I/O + e12-03 降级论述；作业以 Java DataStream 为主（与 p03 工程形态一致），**不以 SQL `ML_PREDICT` 脚本作为验收主路径**。
- **D-02:** **Flink Agents 0.3.0 Preview 不作为 LOG-03 验收硬依赖**：若做展示模块，必须独立 Maven 工程/profile，禁止挂入 p01 主 pom 或阻塞 `mvn clean package`（延续 e12-07 纪律）。本 Phase MVP 可不交付 Agents 作业。
- **D-03:** **Milvus / VECTOR_SEARCH / Streaming RAG** 作为 **可选增强路径**（依赖现有 `docker` `--profile ai` + 宿主机 embedding 模型），可写在降级清单「启用 AI+向量」节；**不**作为 LOG-03 唯一硬依赖。若实现，须独立 verify 目标（如 `verify-rag`），失败不得拖垮默认 `verify`。
- **D-04:** 作业默认 **`--ai.enabled=false`（或等价 JobConfig）**：关闭时只跑解析 → 富化/特征 → 规则告警/标签，**零** Ollama/Milvus 调用；开启时才走 Async AI 旁路。禁止「无 Ollama 则作业直接失败」作为默认行为。
- **D-05:** 文档必须提供 **显式降级核对清单**（勾选表）：无 Ollama / 无 Milvus / AI 关闭 / AI 超时降级到规则，各对应可执行命令与预期可观察输出。
- **D-06:** 验收双轨：**(1) `verify`（默认）** — 仅规则/特征路径，ClickHouse（或等价权威表）断言结构化富化/规则结果，**不依赖** Ollama；(2) **`verify-ai`** — 要求宿主机 Ollama 可用 + `--ai.enabled=true`，断言 AI 分级结果可观察。主 CI/Phase 门禁以 `verify` 为准；`verify-ai` 为本机可选硬验收（LOG-03），文档标明前置。
- **D-07:** 目录与模块名锁定 **`projects/p01-log-ai-platform`**（与 `docs/README.md` 15-01 登记一致）；**独立 pom**，不挂入 `examples/` 父工程。
- **D-08:** Compose：**独立 `--profile p01`**（topic 初始化 / p01-init / 项目专用挂载）；**禁止**污染 default `make up`。Milvus 继续走既有 `--profile ai`，p01 文档说明组合启动方式，不把 milvus 绑进 default。
- **D-09:** 事件契约：合成 **结构化 JSON 日志**（至少含 `service`、`level`、`message`、`traceId`、事件时间戳）；Kafka topic 建议 `logs.events`（可由 planner 微调命名，须幂等创建）；造数脚本提供可判定 scenario（规则命中 / AI 分级可观察）。
- **D-10:** **ClickHouse 为权威验收出口**（富化结果/规则告警/AI 分级落库表）；Kafka 仅诊断。延续 p03 verify 纪律：不以「Kafka 有消息」单独放行。
- **D-11:** 成本与护栏落地为 **Flink 自定义业务指标**（复用父 pom `flink-metrics-dropwizard` 思路 + e12-17/e12-18 概念）：至少可观察 **AI 调用次数、超时/降级数、规则拦截/BLOCK 数、估算 token 或预算熔断触发次数** 之一组；经现有 Prometheus scrape → Grafana/日志可核对。禁止引入未登记的完整 AI Gateway / Loki/OTel 栈（P5 再评估）。
- **D-12:** 护栏 MVP：**输出侧关键词/级别黑名单**（Broadcast 或静态配置均可，优先类比 e12-17）；成本 MVP：**窗口内预算或调用上限熔断后降级到规则路径**（类比 e12-18）。二者须在 README「验证」节有可观察证据命令。
- **D-13:** 压测为 **p01 项目级**（非 `benchmark/` 全矩阵）：造数 `--rate/--duration` + `loadtest` → `docs/baseline.md`；口径对齐 p03 / `benchmark/README.md` 子集；数字仅 OrbStack arm64 实测。
- **D-14:** 可执行演练 **恰好 2 条**（MVP）：(1) **AI 不可用/关闭时降级路径仍绿**（LOG-02 硬演练）；(2) **负载压测跑 baseline**。额外 chaos（杀 TM、断 Kafka、watermark 停滞）可写可选附录，不挡 Phase 完成。
- **D-15:** 文档包：至少 **1 篇 ADR**（主题锁定 **「AI 路径可降级：主构建零硬依赖 Preview/外部模型」**）；`docs/ARCHITECTURE.md` + `docs/RESUME.md`；回填 `docs/README.md` 模块 **15-01** 完成态；README 八段式。验证脚本 + make 目标一键可跟。

### Claude's Discretion
- 具体表名/列名、Async 超时毫秒、Ollama 模型名（须在文档与 JobConfig 默认可改）、护栏关键词列表、Grafana 是否单独 p01 dashboard JSON（可复用 Prometheus 面板 + CH 表查询）、Agents 是否作为附录 demo 目录。
- 压测默认 eps/时长由 executor 在 OrbStack 可稳定跑通前提下实测填写。

### Deferred Ideas (OUT OF SCOPE)
- Flink Agents 多 Agent 分诊/专家协作完整网关 — 属 FUT / 后续里程碑；本 Phase 不挡
- SQL `ML_PREDICT` 作为第二主路径深讲 — 已有 e12-03；p01 主路径用 Async DataStream
- 仓库级 monitoring 三块看板终稿、benchmark 全矩阵 — Phase 6（PROD-*）
- p02 实时推荐 — Phase 5
- Loki/OTel 全链路 Trace 栈 — 超出 LOG-04 MVP
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| LOG-01 | 独立 compose profile 一键起，端到端结构化日志流可复现 | `p01-init` 镜像 `KAFKA_IMAGE` + topics + CH DDL；`gen_log_events.py` → Kafka → Flink → CH；Makefile `up-p01` / `submit` / `verify`（§ Standard Stack / Architecture） |
| LOG-02 | 无 LLM/Milvus 时富化/特征路径仍可完整演示 | 默认 `--ai.enabled=false`；Keyed State / 规则特征不依赖 Redis/Ollama；`drill-degrade` + 降级清单（D-04/D-05/D-14） |
| LOG-03 | 至少一条 AI 路径可用 + 显式降级核对清单 | Async I/O → Ollama `/api/chat` 风险分级；`verify-ai` 可选；Agents/Milvus 非硬依赖（D-01–D-03/D-06） |
| LOG-04 | 成本/护栏业务指标可在 Prometheus/日志观察 | Flink `MetricGroup` Counter（e12-15 模式）+ 护栏 BLOCK / 预算熔断（e12-17/18）；Prometheus `:9249` 已 scrape（D-11/D-12） |
| LOG-05 | 压测、演练、架构、ADR、验证脚本、简历页 | 复制 p03：`loadtest.sh`、`drill_ai_degrade.sh`、ADR-0001 可降级 AI、ARCHITECTURE/RESUME、回填 15-01（D-13–D-15） |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Compose profile / topic+DDL init | Docker `p01-init`（profile=p01） | Kafka + ClickHouse | 对齐 p03-init；禁止写入 default `init.sh` |
| 结构化日志解析 / 事件契约 | Flink Job（Parse JSON） | Kafka `logs.events` | D-09；脏数据丢弃，不拖垮作业 |
| 富化 / 流式特征（无 AI） | Flink Keyed State / 规则 Map | ClickHouse `log_results` | LOG-02 硬路径；MVP **不**绑 Redis（见 Discretion 建议） |
| 规则告警 / 标签 | Flink ProcessFunction | CH `rule_label` 列 | 默认可断言；scenario 造数可判定 |
| AI 风险分级旁路 | Flink Async I/O → 宿主机 Ollama | CH `ai_risk` / `ai_source` | D-01；仅 `--ai.enabled=true` |
| 输出护栏（关键词/级别） | Flink Broadcast 或静态配置 | Metrics `guardrail_blocks` | D-12；类比 e12-17 |
| 成本/调用预算熔断 | Flink 窗口或计数状态 | Metrics `budget_trips` + 降级到规则 | D-12；类比 e12-18 |
| 权威验收出口 | ClickHouse HTTP 查询 | Kafka 仅诊断 | D-10；复制 `verify.sh` 纪律 |
| 可观测（成本/护栏） | Flink Prometheus reporter → Prometheus | Grafana / curl PromQL | D-11；已有 `:9249` scrape |
| 造数 / 压测 / 演练 | Host Python（uv）+ bash | Kafka / Prom / CH | D-13/D-14；无 k6/JMeter |
| 文档包 / 简历 | Repo markdown | `docs/README.md` 15-01 | D-15 |

## Project Constraints (from .cursor/rules/)

No `.cursor/rules/` directory present in this repo. Effective constraints from `CLAUDE.md` / PROJECT invariants:

- Version SSOT：根 README 版本矩阵 + pom 属性区；新增组件先登记再使用
- 文档编号：先在 `docs/README.md` 登记；八段式结构强制
- 运行环境：一切代码/命令必须在 OrbStack arm64 实测通过
- 内容禁令：禁止 TODO、省略、略、自行实现、请参考官网
- 验收：Phase 结束跑 `scripts/qa_check.sh`
- Tech stack：Flink 2.2.1、JDK 21、Kafka 3.9.1；Ollama 宿主机、Milvus `--profile ai`
- p01 独立 pom；default `make up` 不被 `--profile p01` 污染
- ClickHouse = verify 权威出口；Kafka 仅诊断

## Standard Stack

### Core

| Library / Component | Version | Purpose | Why Standard |
|---------------------|---------|---------|--------------|
| Apache Flink | 2.2.1 | DataStream 作业 + Async I/O | SSOT / ADR-001；`[VERIFIED: docker/.env FLINK_IMAGE + examples/pom]` |
| JDK | 21 | 编译/运行；`java.net.http.HttpClient` | SSOT；`[VERIFIED: java -version 21.0.2]` |
| flink-connector-kafka | 5.0.0-2.2 | 消费 `logs.events` | 与 p03 相同；`[VERIFIED: p03 pom + local m2]` |
| jackson-databind | 2.17.2 | JSON 解析 / Ollama body | SSOT（examples/pom）；`[VERIFIED: local m2]` |
| flink-metrics-dropwizard | 2.2.1 | Histogram（可选；Counter 可不依赖） | examples 父 pom 已管；e12-15 用法；`[VERIFIED: local m2 jar]` |
| ClickHouse | 24.8 | 权威落库 + verify | 基座；`[VERIFIED: fll-clickhouse healthy]` |
| Kafka | apache/kafka:3.9.1 | 事件总线 | 基座；`[VERIFIED: docker/.env]` |
| Ollama（宿主机） | 0.9.0+（本机 0.32.1） | `/api/chat` 风险分级 | SSOT；不进容器；`[VERIFIED: ollama --version + JM/TM curl host.docker.internal]` |
| Prometheus / Grafana | v2.53.0 / 11.1.0 | 刮取 Flink `:9249` | `[VERIFIED: docker/.env + prometheus.yml]` |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| ClickHouse HTTP SinkV2（项目内） | 复制 p03 `ClickHouseAlertSink` | 写 `flinklab.log_results` | 禁止新 JDBC 驱动进主路径除非先登记 SSOT |
| `confluent-kafka`（uv script） | ≥2.5 | 造数/压测 | 对齐 p03 gen；`[VERIFIED: slopcheck pypi OK]` |
| Flink `AsyncDataStream.unorderedWaitWithRetry` | 2.2.1 | AI 外呼 | e11-C2 + Flink 2.2 docs；`[CITED: nightlies.apache.org/flink/.../asyncio/]` |
| Ollama `POST /api/chat` | docs.ollama.com | 非流式 JSON 分级 | `"stream":false`；可选 `"format":"json"`；`[CITED: docs.ollama.com/api/chat]` + 本机实测 |
| JUnit 5 | 5.10.2 | 规则/解析/预算单测 | 对齐 p03；Wave 0 |

### Alternatives Considered（CONTEXT 已锁，仅记录勿重开）

| Instead of（locked） | Could Use | Tradeoff / Verdict |
|----------------------|-----------|-------------------|
| Async DataStream → Ollama | SQL `ML_PREDICT` | e12-03 已有教材；参数随版本漂移；**拒绝作验收主路径**（D-01） |
| Async DataStream → Ollama | Flink Agents 0.3 Preview | Preview 阻塞主构建风险；**非硬依赖**（D-02）；可附录 |
| 规则特征（Keyed State） | Redis 特征库（e12-06） | Redis 在 default compose 但当前常未起；绑 Redis 增加 LOG-02 失败面；**MVP 推荐 Keyed State，Redis 可选附录** `[ASSUMED: 产品取舍]` |
| Flink custom metrics | Loki/OTel/AI Gateway | 超出 LOG-04 / 未进 SSOT；**拒绝**（D-11） |
| gen `--rate/--duration` | k6/JMeter | p03 D-07 已禁；p01 同样不引入 |

**Installation / wiring（无新未登记 Maven 坐标）：**

```bash
# 基座（不变）
cd docker && make up && make init

# p01 profile（规划落点，对齐 p03）
# docker/Makefile 增加: up-p01 → docker compose --profile p01 up -d p01-init
cd projects/p01-log-ai-platform && mvn -q clean package
# submit: docker compose exec jobmanager flink run -d -c ... /opt/flink/usrlib/p01-*.jar \
#   --ai.enabled=false
```

**Version verification notes:**
- Flink / Kafka connector / jackson / dropwizard：本地 `~/.m2` 已有对应 jar（2026-07-18）。
- Ollama：本机 `0.32.1`；模型清单 **无** `qwen3:8b`——planner/executor 须在 README 写「默认模型 JobConfig 可改；首次 `verify-ai` 前 `ollama pull <model>` 或传 `--ai.model=qwen3.5:9b-mlx`」。
- slopcheck：`confluent-kafka` pypi = OK；Maven 坐标查询对带版本的 GAV 返回 REGISTRY_ERROR（工具限制），以 SSOT + 本地 m2 为准，**不得**把已锁定的 Apache/FasterXML 工件标为 SLOP。

## Package Legitimacy Audit

> 本 Phase **不引入**未在根 README / examples pom 登记的新 Maven 坐标。造数脚本复用 p03 的 uv `confluent-kafka`。

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| org.apache.flink:flink-streaming-java:2.2.1 | Maven Central | years（Apache） | n/a | github.com/apache/flink | REGISTRY_ERROR（工具）→ treat as SSOT Approved | Approved — already SSOT |
| org.apache.flink:flink-connector-kafka:5.0.0-2.2 | Maven Central | established | n/a | github.com/apache/flink-connector-kafka | REGISTRY_ERROR → SSOT Approved | Approved |
| com.fasterxml.jackson.core:jackson-databind:2.17.2 | Maven Central | years | n/a | github.com/FasterXML/jackson | mis-scanned as pypi SLOP if wrong ecosystem | Approved — SSOT；**勿**用 `slopcheck install jackson-databind`（默认 PyPI） |
| org.apache.flink:flink-metrics-dropwizard:2.2.1 | Maven Central | with Flink | n/a | apache/flink | n/a | Approved — examples pom |
| confluent-kafka（uv script） | PyPI | established | high | github.com/confluentinc/confluent-kafka-python | OK | Approved |
| JDK `java.net.http.HttpClient` | JDK 21 | built-in | n/a | OpenJDK | n/a | Approved — no install |
| Flink Agents / Milvus SDK | — | — | — | — | — | **Not installed in p01 main pom**（D-02/D-03） |

**Packages removed due to slopcheck [SLOP] verdict:** none（无新包；误扫 jackson-databind@pypi 忽略）
**Packages flagged as suspicious [SUS]:** none

*Maven registry probe via slopcheck 对本机返回 400；合法性以 SSOT + 官方 Apache/Fasterxml 坐标 + 本地已缓存 jar 为准。若 planner 引入任何新 Maven GAV，必须先登记版本矩阵并 `checkpoint:human-verify`。*

## Architecture Patterns

### System Architecture Diagram

```text
[gen_log_events.py] --produce--> Kafka logs.events (host localhost:9094)
        │
        ▼
 Flink JobManager / TaskManager
        │
        ├─ ParseLogJson ──► Enrich/Features (Keyed State 规则特征)
        │                         │
        │                         ▼
        │                  RuleTagger (ERROR/AUTH/… → rule_label)
        │                         │
        │            ┌────────────┴────────────┐
        │            │ ai.enabled=false        │ ai.enabled=true
        │            ▼                         ▼
        │     Pass-through                AsyncDataStream
        │     ai_source=DISABLED          → Ollama /api/chat
        │                                  timeout() → DEGRADED→规则
        │                         │
        │                         ▼
        │                  Guardrail (Broadcast/static keywords)
        │                  BudgetGate (window/call cap → 熔断降级)
        │                         │
        │                         ▼
        │                  ClickHouse SinkV2 → flinklab.log_results
        │                  (optional Kafka logs.alerts 诊断)
        │
        ▼
 Prometheus scrape TM/JM :9249 ← custom counters (p01_*)
        │
 verify.sh      → CH rule_label / enriched 行数（不依赖 Ollama）
 verify-ai.sh   → CH ai_risk IN (HIGH,MEDIUM,LOW) 且 ai_source=AI
 drill-degrade  → AI off / endpoint down → verify 仍绿
 loadtest.sh    → docs/baseline.md
```

### Recommended Project Structure

```text
projects/p01-log-ai-platform/
├── pom.xml                          # 独立模块；shade 主类 LogAiJob
├── Makefile                         # package submit gen verify verify-ai loadtest drill-degrade
├── README.md                        # 八段式 + 降级勾选表
├── sql/
│   └── clickhouse_log_results.sql   # 单表权威出口（CREATE IF NOT EXISTS）
├── scripts/
│   ├── gen_log_events.py            # uv script；scenario + --rate/--duration
│   ├── verify.sh                    # CH 权威；白名单拼 SQL
│   ├── verify_ai.sh                 # 前置：ollama tags + ai.enabled
│   ├── loadtest.sh                  # → docs/baseline.md
│   └── drill_ai_degrade.sh          # LOG-02 硬演练
├── docs/
│   ├── ARCHITECTURE.md
│   ├── RESUME.md
│   ├── baseline.md                  # loadtest 生成
│   ├── DEGRADE-CHECKLIST.md         # 或并入 README §验证
│   └── adr/0001-ai-path-degradable.md
├── monitoring/dashboards/           # 可选 p01 JSON；非必须
└── src/
    ├── main/java/com/flywhl/flinklab/p01/
    │   ├── LogAiJob.java
    │   ├── JobConfig.java           # --ai.enabled 等
    │   ├── model/LogEvent.java      # record：service,level,message,traceId,eventTime
    │   ├── model/LogResult.java     # + ruleLabel, aiRisk, aiSource, features…
    │   ├── ParseLogJson.java
    │   ├── enrich/FeatureEnricher.java
    │   ├── rule/RuleTagger.java
    │   ├── ai/OllamaRiskAsyncFunction.java
    │   ├── guardrail/GuardrailFunction.java
    │   ├── cost/BudgetGateFunction.java
    │   └── sink/ClickHouseLogSink.java
    └── test/java/...                # Parse/Rule/Budget 单测（Wave 0）
```

### docker/ 接线（复制 p03-init，改名）

在 `docker/docker-compose.yml` 追加 `p01-init`：

- `profiles: ["p01"]`
- `image: ${KAFKA_IMAGE}`（不引入新镜像）
- volumes：挂载 `../projects/p01-log-ai-platform/sql/*.sql`
- topics（建议，幂等 `--create --if-not-exists`）：
  - `logs.events`（partitions 4）
  - `logs.alerts`（可选诊断）
  - `logs.guardrail.control`（若用 Broadcast 护栏；静态配置则可省略）
- ClickHouse：`wget --post-file` **单语句** DDL（CH HTTP 禁多语句——p03 已踩坑）
- `docker/Makefile`：`up-p01` **显式** `up -d p01-init`；**不要**改 default `up` 目标

### Pattern 1: JobConfig 手写 `--key`（Flink 2.2 无 ParameterTool）

**What:** 复制 p03 `JobConfig.arg`；增加 AI/护栏/预算字段。  
**When:** 所有可运维参数集中于此。

| Flag | Default | Notes |
|------|---------|-------|
| `--ai.enabled` | `false` | D-04 |
| `--ai.endpoint` | `http://host.docker.internal:11434` | 容器内访问宿主机 |
| `--ai.model` | `qwen3:8b`（文档说明可改） | 本机可覆写为已装模型 |
| `--ai.timeout-ms` | `8000` | Discretion；须 ≥ 重试预算 |
| `--ai.capacity` | `16` | Async 在途上限；本机 Ollama 勿过大 |
| `--ai.retry` | `2` | FixedDelay；总时长 ⊂ timeout |
| `--budget.max-ai-calls` | `120` | 每分钟（或滚动窗口）上限 |
| `--guardrail.keywords` | `ignore safety,exfiltrate,越权` | 或 Broadcast topic |
| `--events-topic` | `logs.events` | |
| `--kafka-bootstrap` | `kafka:9092` | 作业内；造数用 `localhost:9094` |
| `--clickhouse-url` | `http://clickhouse:8123/` | |

### Pattern 2: Async Ollama + timeout 降级（e11-C2）

**What:** `RichAsyncFunction` 在 `asyncInvoke` 内用线程池提交 HTTP（或 JDK `HttpClient.sendAsync`），`ResultFuture.complete`；`timeout()` **完成降级记录而非抛异常**。  
**When:** 仅 `ai.enabled=true` 分支接线。  
**Example:**

```java
// Source: Flink 2.2 Async I/O docs + examples/e11-async-io C2TimeoutRetryJob
AsyncRetryStrategy<LogResult> retry = new AsyncRetryStrategies
    .FixedDelayRetryStrategyBuilder<LogResult>(2, 200L)
    .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
    .build();

AsyncDataStream.unorderedWaitWithRetry(
        enriched,
        new OllamaRiskAsyncFunction(endpoint, model),
        8, TimeUnit.SECONDS,
        16,
        retry)
    .uid("p01-ai-ollama-risk");
```

Ollama 请求体（本机实测可用）：

```json
{
  "model": "qwen3.5:9b-mlx",
  "stream": false,
  "format": "json",
  "think": false,
  "options": { "temperature": 0, "num_predict": 64 },
  "messages": [
    {
      "role": "system",
      "content": "Classify log risk. Reply JSON {\"risk\":\"HIGH|MEDIUM|LOW\"} only."
    },
    {
      "role": "user",
      "content": "level=ERROR service=auth message=..."
    }
  ]
}
```

解析失败 / 非枚举 → `ai_risk=UNKNOWN`，`ai_source=DEGRADED`（仍落库，作业不重启）。

### Pattern 3: 验收双轨（p03 verify 纪律）

| Target | 权威断言 | 依赖 |
|--------|----------|------|
| `make verify` | `COUNT() FROM flinklab.log_results WHERE rule_label='AUTH_FAIL'`（或白名单标签）≥ 1 | 仅 CH；AI off |
| `make verify-ai` | 同上表 `ai_source='AI' AND ai_risk IN ('HIGH','MEDIUM','LOW')` ≥ 1 | Ollama up + submit `--ai.enabled=true` |
| Kafka | 可选 `diag` 打印 | **永不单独 exit 0** |

### Pattern 4: 指标命名（LOG-04）

在算子 `open()`：

```java
// Source: examples/e12-15-observability ObservableAgentJob
MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("p01");
aiCalls = g.counter("ai_calls");
aiTimeouts = g.counter("ai_timeouts");
aiDegrades = g.counter("ai_degrades");
guardrailBlocks = g.counter("guardrail_blocks");
budgetTrips = g.counter("budget_trips");
```

Prometheus 上 Flink reporter 会带算子前缀；README「验证」节给一条可复制 PromQL / `curl localhost:9090/api/v1/query`，或 TM 日志关键字核对。**不强制**独立 Grafana JSON（Discretion：复用现有 Flink 健康面板 + CH 查询即可）。

### Anti-Patterns to Avoid

- **默认作业硬依赖 Ollama：** 违反 D-04 / PITFALLS「AI 无降级」
- **Agents / Milvus 进主 pom：** 违反 D-02/D-03 / e12-07 纪律
- **Kafka 有消息即放行：** 违反 D-10
- **同步 HTTP 写在 `map`/`processElement` 主路径：** 违反军规；AI 必须 Async I/O
- **`asyncInvoke` 内阻塞等待 Future.get()：** Flink 文档明确破坏异步性
- **Streaming 默认 `stream:true` 却按单 JSON 解析：** 易卡死解析；强制 `stream:false`
- **Thinking 模型不关 think：** 本机实测 `content` 可为空；须 `think:false` 或非 thinking 模型
- **CH HTTP 一次 POST 多语句：** p03 已证失败；DDL 拆文件或拆 POST
- **编造 baseline 数字 / 沙箱伪跑通：** 项目不变量

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| 异步外呼框架 | 自研线程池算子 | `AsyncDataStream` + `timeout()` | 水位/checkpoint/重试已由框架处理 `[CITED: Flink 2.2 asyncio]` |
| LLM HTTP 协议 | 自研 OpenAI SDK 全家桶 | JDK `HttpClient` + jackson + Ollama `/api/chat` | 零新依赖；对齐 D-01 |
| 护栏引擎 | 外挂策略中台 | Broadcast/static 关键词（e12-17） | MVP 足够可演示 |
| 成本网关 | AI Gateway / 计费 SaaS | 窗口调用计数 + Counter（e12-18） | D-11 禁止未登记网关 |
| CH 写入 | 新 JDBC 栈 | 复制 p03 HTTP SinkV2 | 已验证 flush/校验模式 |
| 压测工具链 | k6/JMeter 镜像 | `gen_log_events.py --rate/--duration` | 对齐 p03；无 SSOT |
| Agents 集成 | 主构建挂 Preview | 不交付或独立模块 | D-02 |

**Key insight:** p01 的差异化不是「可调用模型」，而是 **没有模型时故事仍完整**（规则+特征+CH）+ **有模型时多一条可观察旁路且可熔断**——与 FEATURES.md / PITFALLS 一致。

## Common Pitfalls

### Pitfall 1: AI 路径无降级导致 Phase 假完成
**What goes wrong:** README 要求必须 Ollama；无模型则作业失败或 verify 全红。  
**Why:** 把可选旁路写成硬依赖。  
**How to avoid:** 默认 `ai.enabled=false`；`verify` 门禁不碰 Ollama；降级清单四格可勾选。  
**Warning signs:** CI 需要本机 GPU/模型；`mvn package` 拉 Agents。

### Pitfall 2: Ollama 从容器不可达 / 模型名不匹配
**What goes wrong:** Async 全超时 → 全 DEGRADED；`verify-ai` 失败。  
**Why:** 用 `localhost:11434`（容器环回）而非 `host.docker.internal`；或默认 `qwen3:8b` 未 pull。  
**How to avoid:** JobConfig 默认 endpoint 用 `host.docker.internal`；submit/`verify-ai` 前 `curl` tags；文档写模型覆盖。  
**Warning signs:** TM 日志 connection refused；本机 `ollama list` 无默认模型。  
**Verified:** 2026-07-18 JM/TM → `host.docker.internal:11434/api/tags` 成功。

### Pitfall 3: Thinking 模型返回空 `content`
**What goes wrong:** 解析永远 UNKNOWN。  
**Why:** qwen thinking 模型把 token 花在 `thinking` 上。  
**How to avoid:** 请求设 `"think": false` + `"format":"json"` + 足够 `num_predict`；本机已验证返回 `{"risk":"MEDIUM"}`。  
**Warning signs:** `done_reason=length` 且 content 空。

### Pitfall 4: Async capacity 打满本机 Ollama
**What goes wrong:** 延迟爆炸、背压、checkpoint 变慢。  
**Why:** capacity 按云端 API 拍脑袋。  
**How to avoid:** 默认 capacity≤16；压测 AI 开启时降 rate；timeout 降级保作业活着。  
**Warning signs:** `ai_timeouts` 飙升；Flink backpressure 高。

### Pitfall 5: CH 多语句 / SQL 注入式 verify
**What goes wrong:** init 失败；verify 被任意 `PATTERN` 注入。  
**How to avoid:** 单语句 DDL；verify 白名单 `rule_label`/`ai_risk` 枚举再拼 SQL（抄 p03 `PATTERN_ID` case）。  
**Warning signs:** p01-init exit≠0；verify 接受任意环境变量进查询。

### Pitfall 6: 伪异步（同步客户端堵 mailbox）
**What goes wrong:** 吞吐塌陷，像没开 Async。  
**Why:** 在 `asyncInvoke` 里同步 `HttpURLConnection` 不经线程池。  
**How to avoid:** `CompletableFuture` / `HttpClient.sendAsync` / 固定大小 pool；对齐 e11 README「真异步」。  
**Warning signs:** 并行度升高吞吐几乎不涨。

## Code Examples

### ClickHouse 权威表（建议 DDL — Discretion 可微调列名）

```sql
-- Source: 对齐 p03 clickhouse_alerts.sql 纪律；单语句供 p01-init POST
CREATE TABLE IF NOT EXISTS flinklab.log_results
(
    service      String,
    level        String,
    message      String,
    trace_id     String,
    event_time   DateTime64(3),
    feature_json String,
    rule_label   String,   -- AUTH_FAIL | ERROR_BURST | NONE | ...
    ai_risk      String,   -- HIGH | MEDIUM | LOW | UNKNOWN | NONE
    ai_source    String,   -- DISABLED | AI | RULE | DEGRADED | BLOCKED
    ingest_time  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, service, trace_id);
```

### 造数 scenario（建议）

| Scenario | 用途 | CH 期望 |
|----------|------|---------|
| `rule-auth-fail` | LOG-02 / verify | `rule_label='AUTH_FAIL'` ≥ 1 |
| `rule-error-burst` | 特征/规则 | `rule_label='ERROR_BURST'` ≥ 1 |
| `ai-risk-high` | verify-ai（含明确高危文案） | `ai_source='AI'` 且 `ai_risk` 可观察 |
| `--rate/--duration` | loadtest | baseline 吞吐/lag 表 |

事件 JSON 最小字段：`service`,`level`,`message`,`traceId`,`eventTime`（epoch ms）。

### verify.sh 权威条件（伪代码）

```bash
# Source: projects/p03-vehicle-monitoring/scripts/verify.sh 模式
# 白名单 RULE_LABEL 后：
SELECT count() FROM flinklab.log_results WHERE rule_label='AUTH_FAIL'
# count >= MIN_COUNT → ok；Kafka 仅 diag
```

### Metrics 观察命令（README 可抄）

```bash
curl -sgG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=flink_taskmanager_job_task_operator_p01_ai_calls'
# 若命名被 reporter 改写：先 curl JM/TM :9249/ 或 Flink UI 确认实际 metric 名后写入 README
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| SQL `ML_PREDICT` 作「日志 AI」主叙事 | DataStream Async + Ollama HTTP | Phase 4 CONTEXT D-01 | 与 p03 工程形态一致；降级可控 |
| Agents Preview 绑主构建 | standalone / 不交付 | P3 e12-07 → D-02 | 主 `mvn package` 永不受 Preview 阻塞 |
| 同步算子调 LLM | Async I/O + timeout 降级 | e11 / ai/03 | 避免重启风暴 |
| 无成本可见性的 Demo | Counter + 预算熔断 | e12-18 → LOG-04 | 简历可陈述生产 AI 纪律 |

**Deprecated/outdated for this phase:**
- 以 SQL AI 脚本作为 p01 验收主路径
- 容器内 Ollama
- 未降级的「必须 Milvus」叙事

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | MVP 富化用不依赖 Redis 的 Keyed State/规则特征即可满足 LOG-02 | Standard Stack / Architecture | 若用户坚持 Redis 演示，需加 health 依赖与 e12-06 接线 |
| A2 | 单表 `flinklab.log_results` 足够同时承载规则与 AI 列 | Code Examples | 若列膨胀，可拆 alerts 表但不改 CH 权威原则 |
| A3 | 默认 `--ai.timeout-ms=8000`、`capacity=16` 在本机 9B 模型可演示 | JobConfig | 大模型（35B）需更大 timeout / 更小 capacity；由 executor 实测调 |
| A4 | 不强制独立 Grafana dashboard JSON 也能满足 LOG-04（Prom + CH） | Discretion | 若验收方要求可见面板，补最小 JSON（抄 p03 挂载模式） |
| A5 | LOG-03 条文中的「ML_PREDICT / Agents / Milvus」由 CONTEXT 收窄为 Async Ollama，满足里程碑意图 | phase_requirements | 文档须显式写清「REQ 原文枚举 → 本项目实现选型」以免审计歧义 |

## Open Questions (RESOLVED)

1. **默认 AI 模型名写 SSOT `qwen3:8b` 还是本机已装模型？**
   - RESOLVED: JobConfig 默认 **`qwen3:8b`**（对齐 README SSOT）；README 表格写本机覆盖示例（如 `--ai.model=qwen3.5:9b-mlx`）；`verify-ai` 先探测 tags，缺失则打印 `ollama pull` / 覆写提示后非 0。

2. **护栏用静态配置还是 Broadcast topic？**
   - RESOLVED: MVP **静态关键词列表进 JobConfig**（实现快、单测易）；Broadcast 热更新不挡 Phase（可作加分项，非 must_haves）。

3. **是否交付可选 `verify-rag`？**
   - RESOLVED: **MVP 不交付** RAG 作业 / `verify-rag`；DEGRADE-CHECKLIST 仅指针到 e12-04 + `--profile ai`，避免拖垮 Phase。

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| JDK 21 | 编译 | ✓ | 21.0.2 | — |
| Maven | package | ✓ | 3.9.14 | — |
| Docker / OrbStack | compose | ✓ | Docker 29.4 / orbstack context | — |
| Kafka / Flink / CH / Prom / Grafana | E2E | ✓ | 基座 healthy（抽样） | — |
| uv + Python 3.12 | 造数 | ✓ | uv 0.10.10 / Py 3.12.13 | — |
| Ollama 宿主机 | verify-ai / LOG-03 | ✓ | 0.32.1 | verify 不依赖；清单勾「无 Ollama」 |
| Ollama 模型 `qwen3:8b` | 默认 model | ✗（未 pull） | — | `--ai.model=qwen3.5:9b-mlx` 或 `ollama pull` |
| host.docker.internal from JM/TM | AI 路径 | ✓ | 实测 tags JSON | — |
| Redis | 可选特征 | ✗（容器未运行） | — | MVP 不用 Redis（A1） |
| Milvus `--profile ai` | 可选 RAG | 未在本次强制 up | v2.6.19 镜像已登记 | 不挡 verify |
| Context7 MCP / ctx7 CLI | 文档检索 | ✗ | — | WebFetch 官方 Flink/Ollama docs |
| k6 / JMeter | — | n/a | — | **禁止引入** |

**Missing dependencies with no fallback:**
- 无阻塞项（Ollama 模型名不匹配仅影响 `verify-ai`，有覆写/pull 路径）

**Missing dependencies with fallback:**
- `qwen3:8b` → 覆写模型或 pull
- Redis → Keyed State 特征
- Grafana p01 JSON → PromQL + CH 查询证明 LOG-04

## Validation Architecture

> `workflow.nyquist_validation` = true（`.planning/config.json`）

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 5.10.2（对齐 p03） |
| Config file | surefire in p01 `pom.xml`（新建） |
| Quick run command | `cd projects/p01-log-ai-platform && mvn -q -Dtest=RuleTaggerTest,ParseLogJsonTest,BudgetGateTest test` |
| Full suite command | `cd projects/p01-log-ai-platform && mvn -q test` |
| E2E（OrbStack） | `make verify` / `make verify-ai` / `make drill-degrade` / `make loadtest` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| LOG-01 | profile 起 topic+DDL；E2E 落库 | smoke/e2e | `bash scripts/verify.sh` after submit+gen | ❌ Wave 0 |
| LOG-02 | AI off 时规则/特征可断言 | e2e + unit | `ai.enabled=false` + verify；`RuleTaggerTest` | ❌ Wave 0 |
| LOG-03 | AI on 时 ai_risk 可观察；降级清单 | e2e manual-opt | `bash scripts/verify_ai.sh`；清单 markdown | ❌ Wave 0 |
| LOG-04 | Counter 递增可查 | unit + smoke | 单测 inc；Prom/日志命令 | ❌ Wave 0 |
| LOG-05 | loadtest/drill/docs | script e2e | `loadtest.sh`；`drill_ai_degrade.sh`；文件存在门禁 | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `mvn -q test`（p01 模块）
- **Per wave merge:** `mvn -q test` + `bash -n scripts/*.sh`
- **Phase gate:** `verify` 绿 + 文档包存在 + `scripts/qa_check.sh`；`verify-ai` 本机可选但 LOG-03 文档须可跟

### Wave 0 Gaps

- [ ] `projects/p01-log-ai-platform/` 整树尚不存在
- [ ] `src/test/java/.../ParseLogJsonTest.java` — LOG-01/02
- [ ] `src/test/java/.../RuleTaggerTest.java` — LOG-02
- [ ] `src/test/java/.../BudgetGateTest.java` — LOG-04（纯函数/状态逻辑，无需 MiniCluster）
- [ ] `scripts/verify.sh` / `verify_ai.sh` / `drill_ai_degrade.sh` / `loadtest.sh`
- [ ] `docker` `p01-init` + `make up-p01`
- [ ] 单测不强制 MiniCluster/Testcontainers（与 p03 Gate 单测风格一致）

## Security Domain

> `security_enforcement` = true；ASVS level 1

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | no | 学习工程本地凭据；不新增对外 AuthN |
| V3 Session Management | no | — |
| V4 Access Control | partial | CH/Kafka 仅本地 compose 网络；勿对公网暴露 |
| V5 Input Validation | yes | JSON 解析丢弃脏数据；CH Sink 拒引号/反斜杠（抄 p03）；verify SQL 白名单枚举 |
| V6 Cryptography | no | 不手写加密；无新密钥体系 |

### Known Threat Patterns for Flink + LLM log pipeline

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| 造数/控制面字符串拼进 CH SQL | Tampering | 白名单枚举；Sink 转义/拒用字符 |
| 日志内容注入护栏绕过 | Tampering | 关键词护栏在输出侧；不把模型输出当可信代码 |
| Prompt 注入导致高危标签外泄 | Information Disclosure | 护栏 BLOCK + `ai_source=BLOCKED` 落库；不回写敏感原文到公开 topic（MVP 可截断 message） |
| 预算绕过打爆本机 Ollama | Denial of Service | `budget.max-ai-calls` + Async capacity + 默认 AI off |
| Preview Agents 供应链/构建投毒面 | Tampering | 不进主 pom（D-02） |
| 凭据进 git | Information Disclosure | 复用 compose 已有 `flinklab` 演示口令；不新增云 API key |

## What Planner Should NOT Invent

1. 第二套主 AI 路径（ML_PREDICT 作业、Agents 主验收、Milvus-only LOG-03）
2. 新压测栈（k6/JMeter）、Loki/OTel、AI Gateway
3. 把 Milvus 绑进 `--profile p01` 或 default `make up`
4. 以 Kafka 消费成功作为 verify exit 0
5. 未登记的 Maven 坐标 / Flink 2.3
6. 编造 baseline 数字或「沙箱已验证」勾选
7. 目录名漂移（`p01-log` / `p01-ai`）；必须以 `p01-log-ai-platform` 为准
8. 水平切片「先全部文档后无作业」——应用 **Vertical MVP**：骨架可跑 → 规则 verify → AI verify-ai → 指标/演练/文档包

## Suggested Vertical MVP Slices（供 planner 切 PLAN）

| Slice | 交付物 | 解锁 REQ |
|-------|--------|----------|
| V1 骨架 | pom、LogEvent、JobConfig、p01-init、空作业/解析透传、Makefile package/submit | LOG-01 启动面 |
| V2 规则路径 | FeatureEnricher、RuleTagger、CH Sink、gen scenarios、`verify` | LOG-01 + LOG-02 |
| V3 AI 旁路 | OllamaRiskAsyncFunction、接线开关、`verify-ai`、降级清单 | LOG-03 |
| V4 护栏+预算+指标 | Guardrail、BudgetGate、Counters、README 观察命令 | LOG-04 |
| V5 演练+文档 | loadtest、drill-degrade、ADR、ARCHITECTURE、RESUME、15-01、qa_check | LOG-05 |

## Sources

### Primary (HIGH confidence)

- [CITED: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/asyncio/] — unorderedWaitWithRetry、timeout、capacity、伪异步警告
- [CITED: https://docs.ollama.com/api/chat] — `/api/chat`、`stream:false`、`format`/`think`
- [VERIFIED: OrbStack 本机] — JM/TM → `host.docker.internal:11434`；`think:false`+`format:json` 返回 `{"risk":"MEDIUM"}`
- [VERIFIED: codebase] — p03-init / verify.sh / JobConfig / ClickHouseAlertSink / e11-C2 / e12-15/17/18 / docker prometheus scrape
- [VERIFIED: README / docker/.env] — 版本矩阵 SSOT

### Secondary (MEDIUM confidence)

- [CITED: ai/chapters/03-streaming-inference.md] — 红线（限流/超时降级/成本）与 Async 降级论述
- [CITED: examples/e12-03 README] — ML_PREDICT 降级到 Async HTTP 的官方叙述对照
- [CITED: .planning/research/PITFALLS.md] — AI 无降级陷阱
- [ASSUMED] — MVP 不绑 Redis / 不做 verify-rag（产品取舍，见 Assumptions）

### Tertiary (LOW confidence)

- Flink Prometheus reporter 导出自定义 Counter 的**最终 PromQL 指标名**随 reporter 标签改写——须在第一次 submit 后从 `:9249` 或 Prometheus 实测回填 README（勿在计划中写死未验证的全名）

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — SSOT + 本地 jar/compose/Ollama 实测
- Architecture: HIGH — p03 样板可机械复制；AI 旁路接线清晰
- Pitfalls: HIGH — 仓库 PITFALLS + 本机 thinking 模型/空 content 实测
- Agents/RAG 细节: LOW — 本 Phase 故意不深挖（非硬依赖）

**Research date:** 2026-07-18
**Valid until:** 2026-08-18（Flink/Ollama API 稳定；模型名以本机 `ollama list` 为准需每次核对）
