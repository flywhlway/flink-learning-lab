# Phase 4: p01 日志 AI 平台 - Context

**Gathered:** 2026-07-18
**Status:** Ready for planning

<domain>
## Phase Boundary

交付 `projects/p01-log-ai-platform` 生产级项目：独立 compose profile 一键起、**无 AI 依赖时可完整演示**的结构化日志解析/富化/特征/规则路径、**至少一条可观察的 AI 路径**（可显式降级）、成本/护栏业务指标可观测，以及压测、故障演练、架构/ADR、验证脚本与简历陈述页——使 p01 达到与 p03 同等的单项目完成态。

本 Phase 对应 REQUIREMENTS：**LOG-01**（profile + 端到端结构化日志流）、**LOG-02**（无 LLM/Milvus 时特征路径仍可演示）、**LOG-03**（至少一条 AI 路径 + 降级核对清单）、**LOG-04**（成本/护栏指标可观察）、**LOG-05**（压测、演练、架构/ADR、验证脚本、简历页）。

不在本 Phase：p02 推荐闭环、P5 全矩阵 benchmark / Operator Blue-Green / 仓库级 monitoring 三块看板终稿、完整多 Agent 协作网关、云托管作为主演示路径。

</domain>

<decisions>
## Implementation Decisions

### AI 主路径选型（LOG-03）
- **D-01:** 主 AI 路径锁定为 **DataStream Async I/O → 宿主机 Ollama HTTP（`/api/chat`）做日志风险分级**（HIGH/MEDIUM/LOW 或等价枚举）。对齐 e11 Async I/O + e12-03 降级论述；作业以 Java DataStream 为主（与 p03 工程形态一致），**不以 SQL `ML_PREDICT` 脚本作为验收主路径**。
- **D-02:** **Flink Agents 0.3.0 Preview 不作为 LOG-03 验收硬依赖**：若做展示模块，必须独立 Maven 工程/profile，禁止挂入 p01 主 pom 或阻塞 `mvn clean package`（延续 e12-07 纪律）。本 Phase MVP 可不交付 Agents 作业。
- **D-03:** **Milvus / VECTOR_SEARCH / Streaming RAG** 作为 **可选增强路径**（依赖现有 `docker` `--profile ai` + 宿主机 embedding 模型），可写在降级清单「启用 AI+向量」节；**不**作为 LOG-03 唯一硬依赖。若实现，须独立 verify 目标（如 `verify-rag`），失败不得拖垮默认 `verify`。

### 降级开关与验收双轨（LOG-02 / LOG-03）
- **D-04:** 作业默认 **`--ai.enabled=false`（或等价 JobConfig）**：关闭时只跑解析 → 富化/特征 → 规则告警/标签，**零** Ollama/Milvus 调用；开启时才走 Async AI 旁路。禁止「无 Ollama 则作业直接失败」作为默认行为。
- **D-05:** 文档必须提供 **显式降级核对清单**（勾选表）：无 Ollama / 无 Milvus / AI 关闭 / AI 超时降级到规则，各对应可执行命令与预期可观察输出。
- **D-06:** 验收双轨：**(1) `verify`（默认）** — 仅规则/特征路径，ClickHouse（或等价权威表）断言结构化富化/规则结果，**不依赖** Ollama；(2) **`verify-ai`** — 要求宿主机 Ollama 可用 + `--ai.enabled=true`，断言 AI 分级结果可观察。主 CI/Phase 门禁以 `verify` 为准；`verify-ai` 为本机可选硬验收（LOG-03），文档标明前置。

### 工程骨架与权威出口（LOG-01 / 对齐 p03）
- **D-07:** 目录与模块名锁定 **`projects/p01-log-ai-platform`**（与 `docs/README.md` 15-01 登记一致）；**独立 pom**，不挂入 `examples/` 父工程。
- **D-08:** Compose：**独立 `--profile p01`**（topic 初始化 / p01-init / 项目专用挂载）；**禁止**污染 default `make up`。Milvus 继续走既有 `--profile ai`，p01 文档说明组合启动方式，不把 milvus 绑进 default。
- **D-09:** 事件契约：合成 **结构化 JSON 日志**（至少含 `service`、`level`、`message`、`traceId`、事件时间戳）；Kafka topic 建议 `logs.events`（可由 planner 微调命名，须幂等创建）；造数脚本提供可判定 scenario（规则命中 / AI 分级可观察）。
- **D-10:** **ClickHouse 为权威验收出口**（富化结果/规则告警/AI 分级落库表）；Kafka 仅诊断。延续 p03 verify 纪律：不以「Kafka 有消息」单独放行。

### 成本/护栏可观测（LOG-04）
- **D-11:** 成本与护栏落地为 **Flink 自定义业务指标**（复用父 pom `flink-metrics-dropwizard` 思路 + e12-17/e12-18 概念）：至少可观察 **AI 调用次数、超时/降级数、规则拦截/BLOCK 数、估算 token 或预算熔断触发次数** 之一组；经现有 Prometheus scrape → Grafana/日志可核对。禁止引入未登记的完整 AI Gateway / Loki/OTel 栈（P5 再评估）。
- **D-12:** 护栏 MVP：**输出侧关键词/级别黑名单**（Broadcast 或静态配置均可，优先类比 e12-17）；成本 MVP：**窗口内预算或调用上限熔断后降级到规则路径**（类比 e12-18）。二者须在 README「验证」节有可观察证据命令。

### 压测、演练与文档包（LOG-05）
- **D-13:** 压测为 **p01 项目级**（非 `benchmark/` 全矩阵）：造数 `--rate/--duration` + `loadtest` → `docs/baseline.md`；口径对齐 p03 / `benchmark/README.md` 子集；数字仅 OrbStack arm64 实测。
- **D-14:** 可执行演练 **恰好 2 条**（MVP）：(1) **AI 不可用/关闭时降级路径仍绿**（LOG-02 硬演练）；(2) **负载压测跑 baseline**。额外 chaos（杀 TM、断 Kafka、watermark 停滞）可写可选附录，不挡 Phase 完成。
- **D-15:** 文档包：至少 **1 篇 ADR**（主题锁定 **「AI 路径可降级：主构建零硬依赖 Preview/外部模型」**）；`docs/ARCHITECTURE.md` + `docs/RESUME.md`；回填 `docs/README.md` 模块 **15-01** 完成态；README 八段式。验证脚本 + make 目标一键可跟。

### Claude's Discretion
- 具体表名/列名、Async 超时毫秒、Ollama 模型名（须在文档与 JobConfig 默认可改）、护栏关键词列表、Grafana 是否单独 p01 dashboard JSON（可复用 Prometheus 面板 + CH 表查询）、Agents 是否作为附录 demo 目录。
- 压测默认 eps/时长由 executor 在 OrbStack 可稳定跑通前提下实测填写。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap / Requirements
- `.planning/ROADMAP.md` — Phase 4 目标与成功标准（LOG-01–05）
- `.planning/REQUIREMENTS.md` — LOG-01 … LOG-05 条文
- `.planning/PROJECT.md` — 里程碑不变量（OrbStack 实测、禁伪跑通、SSOT）
- `.planning/STATE.md` — p03 已锁定工程纪律（独立 pom、profile 隔离、CH 权威、verify 双写纪律）
- `.planning/phases/03-p03/03-CONTEXT.md` — 压测/演练/文档包样板决策（本 Phase 复制纪律、换日志 AI 域）

### Architecture / Stack / Pitfalls
- `.planning/research/ARCHITECTURE.md` — p01 日志 AI 示意；compose profile 隔离
- `.planning/research/STACK.md` — p01 栈要点（Kafka→富化→可选 AI；Agents Preview；Ollama 宿主机；Milvus ai-profile）
- `.planning/research/PITFALLS.md` — 「AI 路径无降级」陷阱；p01 强制降级 must-have
- `.planning/research/FEATURES.md` — p01 差异化（降级 + 成本熔断同演示）
- `.planning/research/SUMMARY.md` — 构建顺序与 p01 验收要点
- `PHASES.md` — P4 单项目完成态验收口径
- `README.md` — 版本矩阵 SSOT、ADR-001、端口速查

### AI 教材与 Demo（机制样板）
- `ai/README.md` — AI 专书索引与降级纪律总览
- `ai/chapters/03-streaming-inference.md` — 推理红线与 Async 降级论述
- `ai/chapters/04-streaming-embedding-vector.md` — embedding/向量（可选 RAG）
- `ai/chapters/05-streaming-rag.md` — Streaming RAG（可选）
- `ai/chapters/07-agent-quickstart.md` — Agents Preview；主构建隔离
- `ai/chapters/17-streaming-guardrail.md` — 护栏（e12-17）
- `ai/chapters/18-streaming-cost-control.md` — 成本熔断（e12-18）
- `ai/chapters/24-reference-architecture.md` — 参考架构（叙事边界，勿一次铺满 24 章）
- `examples/e11-async-io/` — Async I/O 主 AI 路径技术基座
- `examples/e12-03-streaming-inference/` — ML_PREDICT/Ollama；降级到 Async 的官方叙述对照
- `examples/e12-04-streaming-inference-vector/` — Milvus 可选路径 + `make up-ai`
- `examples/e12-07-agent-quickstart/` — Agents standalone 隔离样板（勿挂主构建）
- `examples/e12-17-streaming-guardrail/` — Broadcast 护栏样板
- `examples/e12-18-streaming-cost-control/` — 预算熔断样板
- `examples/e12-06-streaming-feature/` — 流式特征对照

### p03 工程纪律样板（复制点）
- `projects/p03-vehicle-monitoring/README.md` — 八段式 + profile/verify 组织
- `projects/p03-vehicle-monitoring/scripts/verify.sh` — CH 权威断言样板
- `projects/p03-vehicle-monitoring/docs/baseline.md` — 项目级 baseline 口径
- `projects/p03-vehicle-monitoring/docs/adr/` — ADR 形态
- `projects/p03-vehicle-monitoring/docs/RESUME.md` — 简历陈述页形态
- `projects/p03-vehicle-monitoring/docs/ARCHITECTURE.md` — 架构短文形态

### Docker / 可观测 / 编号
- `docker/docker-compose.yml` — milvus `--profile ai`；扩展 `p01` profile 落点
- `docker/Makefile` — `up` / `up-ai` 等既有目标
- `monitoring/README.md` — Flink Prometheus 值班指标（成本指标叠加）
- `benchmark/README.md` — 压测方法论（全矩阵属 P5；本 Phase 取子集）
- `docs/README.md` — 模块 **15-01** 编号登记（须回填完成态）
- `scripts/qa_check.sh` — Phase 结束门禁

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- p03：独立 pom、compose profile、`p03-init`、JobConfig 手写参数、CH 权威 verify、loadtest/drill/ADR/RESUME 全套纪律可直接复制到 p01
- e11 Async I/O：主 AI 路径实现模板
- e12-17 / e12-18：护栏与成本熔断教学实现（可迁到生产项目并接真实指标）
- e12-03 / e12-04：Ollama / Milvus 启用步骤与降级说明可回链，不必重写教材
- e12-07：Agents 隔离依赖模式（若附录演示）
- docker `--profile ai`：Milvus 已交付，勿重复造轮子

### Established Patterns
- 主构建零硬依赖 Preview；外部模型走宿主机 Ollama（`host.docker.internal`）
- default `make up` 不被项目 profile 污染
- ClickHouse = 验收权威出口；Kafka 诊断
- 算子 `.uid(...)`；OrbStack arm64 实测才合入
- 文档八段式 + docs/README 编号登记；禁止 TODO/省略/「请参考官网」

### Integration Points
- 新建：`projects/p01-log-ai-platform`（作业、DDL、造数、verify/verify-ai、loadtest、drill-degrade、docs）
- `docker/`：追加 `--profile p01`（topic/init）；不改 default 服务集合语义
- 可选：`--profile ai` 组合文档；Grafana/Prometheus 复用基座
- 回填：docs/README 15-01、PHASES/CHANGELOG（执行期）

</code_context>

<specifics>
## Specific Ideas

- `--auto` 会话：优先「可断言、可降级、可简历陈述」；不追求 Agents 多专家协作或完整 RAG 平台。
- 学习工程叙事：p01 证明「流式 AI 可以没有模型也能讲完故事，有模型时多一条可观察旁路」——与 PITFALLS「AI 无降级」对冲。
- 目录名以 `docs/README.md` 的 `p01-log-ai-platform` 为准，避免 `p01-log` / `p01-ai` 漂移。

</specifics>

<deferred>
## Deferred Ideas

- Flink Agents 多 Agent 分诊/专家协作完整网关 — 属 FUT / 后续里程碑；本 Phase 不挡
- SQL `ML_PREDICT` 作为第二主路径深讲 — 已有 e12-03；p01 主路径用 Async DataStream
- 仓库级 monitoring 三块看板终稿、benchmark 全矩阵 — Phase 6（PROD-*）
- p02 实时推荐 — Phase 5
- Loki/OTel 全链路 Trace 栈 — 超出 LOG-04 MVP

</deferred>

---

*Phase: 4-p01 日志 AI 平台*
*Context gathered: 2026-07-18*
