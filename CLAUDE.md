<!-- GSD:project-start source:PROJECT.md -->
## Project

**flink-learning-lab · P4–P6 里程碑**

企业级 Apache Flink 全栈学习工程（2026 · AI 时代版）：教材 + Demo + 生产级项目 + 最佳实践合集。本里程碑在已交付的 P0–P3 基座上，落地三大生产级项目（车联网监控优先）、生产化能力与总装 QA，使仓库达到「可简历陈述、可一键复现、可压测演练」的完成态。

**Core Value:** 每个生产级项目必须在 OrbStack arm64 上独立 compose profile 一键起、端到端可复现，且压测与故障演练真实跑通——不可验证的内容不合入。

### Constraints

- **版本 SSOT**：根 README 版本矩阵 + `examples/pom.xml` 属性区；新增组件先登记再使用
- **文档编号**：先在 `docs/README.md` 登记；八段式结构强制（背景→架构→代码→启动→验证→踩坑→最佳实践→面试题+参考）
- **运行环境**：一切代码/命令必须在 OrbStack arm64 实测通过；不可验证不合入
- **内容禁令**：禁止 TODO、省略、略、自行实现、请参考官网
- **会话收尾**：更新 CHANGELOG 未发布区 + PHASES.md 状态列；约定式提交（如 `feat(p4): ...`）
- **验收**：每个 Phase 结束跑 `scripts/qa_check.sh`（编译、compose config、断链、违禁词、案例计数）
- **Tech stack**：Flink 2.2.1、JDK 21、Kafka 3.9.1、相关生态见版本矩阵
<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->
## Technology Stack

## Recommended Stack
### Core Technologies（沿用 SSOT，禁止漂移）
| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Apache Flink | 2.2.1 | 作业运行时 | 主线已锁定；ADR-001 禁止升 2.3 |
| JDK | 21 | 编译与运行 | 镜像 `flink:2.2.1-java21`；records/虚拟线程按场景 |
| flink-connector-kafka | 5.0.0-2.2 | 事件总线 IO | 官方兼容 2.2.x |
| Flink CEP | 随 Flink 2.2.1 | p03 模式匹配 | e10 已验证；p03 模式库加深 |
| Flink CDC | 3.6.0 | p01/p02 可选源 | 已交付 e08 |
| Flink Agents | 0.3.0 | p01 日志 AI Agent 路径 | Preview；standalone 隔离依赖 |
| Flink K8s Operator | 1.15.0 | P5 Blue/Green | 官方兼容 Flink 2.2 |
| Kafka | apache/kafka:3.9.1 KRaft | 事件总线 | 已在 docker 基座 |
| ClickHouse | 24.8 | 告警/指标落库 | 已在基座 |
| PostgreSQL | 16-alpine | 维表/配置 | wal_level=logical 已开 |
| Redis | 7-alpine | 特征/缓存 | 推荐与维表缓存 |
| MinIO | latest | Checkpoint/湖仓 | 已在基座 |
| Prometheus/Grafana | v2.53.x / 11.x | 可观测与大盘 | 已接通 Flink 指标 |
| Milvus | v2.6.19 | p01 向量检索 | ai-profile 已交付 |
| Ollama | 0.9.0+ 宿主机 | p01 LLM | 不进容器；host.docker.internal |
### Supporting Libraries（P4 新增，须先登记版本矩阵）
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| flink-cep | 2.2.1 | p03 Pattern API | 模式库 + Broadcast 动态选择 |
| jedis / lettuce | 与 e07 一致 | Redis 特征读写 | p02 在线特征 |
| ClickHouse JDBC/HTTP | 与 e07 SinkV2 一致 | 告警/指标写入 | p03 落库 |
| flink-metrics-dropwizard | 父 pom 已管 | 自定义业务指标 | 三项目统一 |
| Testcontainers（可选） | 最新稳定 | 验证脚本辅助 | 仅本地；不替代 OrbStack 实测 |
### Development / Ops Tools（P5）
| Tool | Purpose | Notes |
|------|---------|-------|
| OrbStack K8s | 本地 K8s | arm64 原生；Blue/Green 演练场 |
| Helm | Operator/作业发布 | 与 GitOps 同路径 |
| Argo CD（或 Flux） | GitOps | 选一个讲透，不双栈 |
| k6 或自定义 Java 压测 | benchmark | 与 scripts/ 数据生成器配合 |
| qa_check.sh | 门禁 | 每 Phase 结束强制 |
## Installation
# 基座
# p03 profile（规划中）
# P5 Operator（规划中，OrbStack K8s）
## Alternatives Considered
| Option | Verdict | Why |
|--------|---------|-----|
| Flink 2.3.0 主线 | 拒绝 | 连接器/Agents/Operator 未齐（ADR-001） |
| StateFun | 拒绝 | 2026-01 社区停运 |
| 自建 ZooKeeper Kafka | 拒绝 | 已用 KRaft |
| 容器内 Ollama | 拒绝 | 48GB 本机原生更稳；已定宿主机方案 |
| 同时上 Argo+Flux | 拒绝 | 学习工程只深讲一条 GitOps 路径 |
| 云厂商托管 Flink 作为演示主路径 | 拒绝 | 目标是本地可复现 |
## What NOT to Use
- **Flink 2.3 connectors「抢先试用」** — 无官方兼容声明，破坏 SSOT
- **未验证的商业 CEP 动态规则引擎** — p03 用 Broadcast + 预编译模式集即可
- **完整多租户 SaaS 控制面** — 超出学习工程范围
- **在沙箱声称「已跑通」但未在 OrbStack 实测的命令** — 违反不变量
## 分项目栈要点
### p03 车联网监控
- DataStream + CEP + Side Output + Kafka + ClickHouse + Grafana
- 复用 e10 C5 模式；Broadcast 切换预编译 Pattern
- Confidence: HIGH
### p01 日志 AI 平台
- Kafka 日志流 → 解析/富化 →（可选）ML_PREDICT / Agents / Milvus RAG
- 降级路径：无 Ollama/Milvus 时仍可跑规则与特征路径（延续 P3 纪律）
- Confidence: MEDIUM（Agents Preview 坐标需本机再验）
### p02 实时推荐
- 行为流 + Redis/PG 特征 + 窗口/关联 + 候选召回写 Kafka
- 模型推理可先用规则/简单打分，LLM 非必须
- Confidence: HIGH
### P5 生产化
- Operator 1.15 + Helm + 单一 GitOps + Prometheus 看板 JSON + benchmark 矩阵
- Confidence: MEDIUM（OrbStack K8s 细节需本机验证）
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
