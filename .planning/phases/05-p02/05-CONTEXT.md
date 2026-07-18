# Phase 5: p02 实时推荐 - Context

**Gathered:** 2026-07-18
**Status:** Ready for planning

<domain>
## Phase Boundary

交付 `projects/p02-realtime-reco` 生产级项目：独立 compose profile 一键起、用户行为流接入、**在线特征（Keyed State + Redis）+ 规则召回/打分闭环**、结果可在 ClickHouse/Kafka 观察，以及压测、故障演练、架构/ADR、验证脚本与简历陈述页——使 p02 达到与 p03/p01 同等的单项目完成态，收齐 P4 三大项目。

本 Phase 对应 REQUIREMENTS：**RECO-01**（独立 compose profile + 行为流接入）、**RECO-02**（在线特征 + 召回/打分闭环，结果可观察）、**RECO-03**（压测、故障演练、架构文档、ADR、验证脚本、简历陈述页）。

不在本 Phase：p01/p03 改造、P5 全矩阵 benchmark / Operator Blue-Green / 仓库级 monitoring 三块看板终稿、真实 ML 模型服务、ANN/向量召回、LLM 重排、多租户推荐中台、云托管作为主演示路径。

</domain>

<decisions>
## Implementation Decisions

### 在线特征存储（RECO-02）
- **D-01:** 在线特征采用 **双通道**（对齐 e12-06 / ai/06）：**(1) Flink Keyed State** 承载会话/窗口内实时累积特征；(2) **Redis**（基座已有 `redis:7-alpine` + e07-C7 jedis Pipeline）作为在线特征库点查侧，供打分阶段低延迟读取。禁止「只写 Redis 不建状态」或「纯状态从不落 Redis」——两者都要可观察，才能讲清新鲜度差异。
- **D-02:** Redis 客户端锁定 **jedis**（与 e07-C7 / e12-06 一致）；版本须先登记根 README 版本矩阵 + 项目/父 pom，禁止漂移到未登记的 lettuce 除非调研证明必须且登记后使用。特征 key 约定形如 `feature:{userId}:*`（可由 planner 微调，须在文档与 verify 中一致）。
- **D-03:** 特征写入须考虑 **at-least-once + 攒批**（复用 e07-C7 CheckpointedFunction/Pipeline 思路）；文档写明 Redis 侧语义与故障边界，禁止假装 exactly-once。

### 召回 / 打分闭环（RECO-02）
- **D-04:** 闭环锁定为 **规则/简单加权打分**（行为亲和、近因、类目匹配等可解释因子）→ **Top-K 候选** 写出。**不**引入 LLM 重排、**不**引入外部模型服务、**不**以 Milvus/ANN 向量召回作为本 Phase 硬依赖（向量路径属 p01/FUT）。
- **D-05:** 候选目录（item catalog）MVP：**合成静态目录**（作业内常量或 `p02-init` 写入 **PostgreSQL** 维表二选一；推荐 PG 维表 + Async/维表关联以便简历可讲「流批维表」）。目录规模学习工程级（数十～数百 item 即可），禁止接真实广告/电商生产数据。
- **D-06:** 输出契约：每条推荐结果至少含 `userId`、`itemId`（或 item 列表/Top-K JSON）、`score`、`ts`、可选 `reason`/`featureSnapshot` 摘要；写入 **Kafka 推荐结果 topic**（建议 `reco.results`）并 **双写 ClickHouse 权威表**（表名可由 planner 定，如 `reco_results`）。

### 工程骨架与权威出口（RECO-01）
- **D-07:** 目录与模块名锁定 **`projects/p02-realtime-reco`**（与 `docs/README.md` 模块 **15-02** 登记一致）；**独立 pom**，不挂入 `examples/` 父工程。
- **D-08:** Compose：**独立 `--profile p02`**（topic 初始化 / `p02-init` / 项目专用挂载）；**禁止**污染 default `make up`。Redis/PG 已在 default 基座，p02 profile 只追加项目专属 init/topic，不重复造 Redis 服务。
- **D-09:** 事件契约：合成 **用户行为 JSON**（至少含 `userId`、`itemId`、`eventType`（view/click/cart/buy 或等价枚举）、事件时间戳）；Kafka topic 建议 `reco.events`（幂等创建）；造数脚本提供可判定 scenario（特征可见 + Top-K 结果可断言）。
- **D-10:** **ClickHouse 为权威验收出口**（推荐结果表行数/样例断言）；Kafka 与 Redis KEYS 仅诊断。延续 p03/p01 verify 纪律：不以「Kafka 有消息」或「Redis 有 key」单独放行。

### 压测、演练与文档包（RECO-03）
- **D-11:** 压测为 **p02 项目级**（非 `benchmark/` 全矩阵）：造数 `--rate/--duration` + `loadtest` → `docs/baseline.md`；口径对齐 p03/p01 / `benchmark/README.md` 子集；数字仅 OrbStack arm64 实测。
- **D-12:** 可执行演练 **恰好 2 条**（MVP）：(1) **Redis 不可用/中断时降级**——作业仍可凭 Keyed State 特征产出可观察推荐结果（或明确降级到「state-only 打分」路径，文档+脚本可断言）；(2) **负载压测跑 baseline**。额外 chaos（杀 TM、断 Kafka）可写可选附录，不挡 Phase 完成。
- **D-13:** 文档包：至少 **1 篇 ADR**（主题锁定 **「在线特征双通道：Flink Keyed State + Redis 点查」vs 纯状态或纯外存**）；`docs/ARCHITECTURE.md` + `docs/RESUME.md`；回填 `docs/README.md` 模块 **15-02** 完成态；README 八段式。验证脚本 + make 目标一键可跟。

### Claude's Discretion
- 具体 CH/PG 表名与列设计、Top-K 默认 K、打分权重公式细节、行为 eventType 枚举命名、是否单独 p02 Grafana dashboard JSON（可复用 Prometheus + CH 查询）、Redis 连接参数默认值。
- 压测默认 eps/时长由 executor 在 OrbStack 可稳定跑通前提下实测填写。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap / Requirements
- `.planning/ROADMAP.md` — Phase 5 目标与成功标准（RECO-01–03）
- `.planning/REQUIREMENTS.md` — RECO-01、RECO-02、RECO-03 条文
- `.planning/PROJECT.md` — 里程碑不变量（OrbStack 实测、禁伪跑通、SSOT）
- `.planning/STATE.md` — p03/p01 已锁定工程纪律（独立 pom、profile 隔离、CH 权威、verify 纪律）
- `.planning/phases/04-p01-ai/04-CONTEXT.md` — 最近项目级样板（profile / verify 双轨 / 演练 2 条 / 文档包）
- `.planning/phases/03-p03/03-CONTEXT.md` — 压测/演练/文档包纪律样板

### Architecture / Stack / Features
- `.planning/research/ARCHITECTURE.md` — p02 示意：行为 → 特征（状态/Redis）→ 召回/打分 → Kafka；compose profile 隔离
- `.planning/research/STACK.md` — p02 栈要点（行为流 + Redis/PG + 规则打分；LLM 非必须）
- `.planning/research/FEATURES.md` — p02 功能表（行为流 / 在线特征 / 候选+打分 / 可观察出口）
- `.planning/research/PITFALLS.md` — 工程纪律复制陷阱；p03→p01/p02 样板依赖
- `.planning/research/SUMMARY.md` — 构建顺序与 p02 验收位置
- `PHASES.md` — P4 单项目完成态验收口径
- `README.md` — 版本矩阵 SSOT、ADR-001、端口速查

### 特征 / 连接器教材与 Demo
- `docs/03-state/README.md` — Keyed State / 状态语义（p02 交叉引用）
- `docs/07-connectors/README.md` — 连接器与外存（Redis/Kafka）
- `ai/chapters/06-streaming-feature.md` — 流式特征双通道论述
- `examples/e12-06-streaming-feature/` — 窗口+MapState→Redis 教学实现
- `examples/e07-connectors/` — C7 jedis Pipeline + CheckpointedFunction 攒批写 Redis
- `examples/e07-connectors/src/main/java/com/flywhl/flinklab/e07/C7RedisBatchWriteJob.java` — 生产复刻基点

### p03 / p01 工程纪律样板（复制点）
- `projects/p03-vehicle-monitoring/README.md` — 八段式 + profile/verify 组织
- `projects/p03-vehicle-monitoring/scripts/verify.sh` — CH 权威断言样板
- `projects/p03-vehicle-monitoring/docs/baseline.md` — 项目级 baseline 口径
- `projects/p03-vehicle-monitoring/docs/adr/` — ADR 形态
- `projects/p03-vehicle-monitoring/docs/RESUME.md` — 简历陈述页形态
- `projects/p03-vehicle-monitoring/docs/ARCHITECTURE.md` — 架构短文形态
- `projects/p01-log-ai-platform/README.md` — 最近完成的项目级样板（降级演练 + verify 组织）
- `projects/p01-log-ai-platform/scripts/verify.sh` — 默认路径权威断言
- `projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh` — 外依赖降级演练对照（p02 换 Redis）
- `projects/p01-log-ai-platform/docs/ARCHITECTURE.md` — 架构文档对照

### Docker / 可观测 / 编号
- `docker/docker-compose.yml` — redis/postgres 基座；扩展 `p02` profile 落点
- `docker/Makefile` — `up` / 既有 profile 目标；勿污染 default
- `monitoring/README.md` — Flink Prometheus 值班指标
- `benchmark/README.md` — 压测方法论（全矩阵属 P5；本 Phase 取子集）
- `docs/README.md` — 模块 **15-02** 编号登记（须回填完成态）
- `scripts/qa_check.sh` — Phase 结束门禁

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- p03/p01：独立 pom、compose profile、`*-init`、JobConfig 手写参数、CH 权威 verify、loadtest/drill/ADR/RESUME 全套纪律可直接复制到 p02
- e12-06：窗口特征 + MapState → Redis 双通道模板
- e07-C7：jedis Pipeline + CheckpointedFunction 容错攒批写
- docker 基座已含 Redis / PostgreSQL / Kafka / ClickHouse / Prometheus / Grafana——p02 不必新增服务镜像（除非 init 容器）

### Established Patterns
- default `make up` 不被项目 profile 污染
- ClickHouse = 验收权威出口；Kafka / Redis 诊断
- 独立 pom；算子 `.uid(...)`；OrbStack arm64 实测才合入
- 文档八段式 + docs/README 编号登记；禁止 TODO/省略/「请参考官网」
- 项目级压测 + 恰好 2 条可执行演练（p03 watermark / p01 AI 降级 → p02 Redis 降级）

### Integration Points
- 新建：`projects/p02-realtime-reco`（作业、DDL、造数、verify、loadtest、drill-redis-degrade、docs）
- `docker/`：追加 `--profile p02`（topic/init）；不改 default 服务集合语义
- 回填：docs/README 15-02、PHASES/CHANGELOG（执行期）

</code_context>

<specifics>
## Specific Ideas

- `--auto` 会话：优先「可断言、可降级、可简历陈述」；不追求 ANN/LLM 重排或完整推荐中台。
- 学习工程叙事：p02 证明「实时特征双通道 + 可解释打分闭环」——与 e12-06 教材形成项目级落地，对冲「只会写 Redis demo」的浅层印象。
- 目录名以 `docs/README.md` 的 `p02-realtime-reco` 为准，避免 `p02-reco` / `p02-recommendation` 漂移。

</specifics>

<deferred>
## Deferred Ideas

- ANN / Milvus 向量召回作为第二召回通道 — 属 FUT / 可与 p01 向量能力交叉；本 Phase 不挡
- LLM 重排 / 生成式推荐 — 超出 RECO MVP；STACK 已声明非必须
- 真实广告/电商生产数据接入 — 合规与范围；用合成数据
- 仓库级 monitoring 三块看板终稿、benchmark 全矩阵 — Phase 6（PROD-*）
- Operator Blue/Green 以 p02 作业为对象 — Phase 6

</deferred>

---

*Phase: 5-p02 实时推荐*
*Context gathered: 2026-07-18*
