# Stack Research

**Domain:** Flink 企业级流处理学习工程 · P4–P6 生产项目与生产化
**Researched:** 2026-07-17
**Confidence:** HIGH（后续里程碑；核心版本已由 README 版本矩阵锁定）

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

核心栈已由 `docker/make up` 提供。项目增量：

```bash
# 基座
cd docker && make up && make init

# p03 profile（规划中）
docker compose --profile p03 up -d

# P5 Operator（规划中，OrbStack K8s）
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
```

新增依赖必须：① 根 README 版本矩阵登记 ② `examples/pom.xml` 或 `projects/*/pom.xml` 属性区同步。

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

---
*Research date: 2026-07-17 · Subsequent milestone · Sources: README 版本矩阵, PHASES.md, e10 README, PROJECT.md*
