# examples/ · 可运行 Demo 工程(Maven 多模块)

统一规则:

- 模块命名 `eNN-<主题>`;所有版本与插件由父 `pom.xml` 统一管理(SSOT),子模块**禁止**声明任何版本号。
- Flink 核心 `provided`;连接器与业务依赖打入 shaded 作业 jar。
- 本地运行:`mvn -Plocal compile exec:java -pl <module> -Dexec.mainClass=<Job>`;集群提交:构建后经 `docker/jobs/` 卷 + `flink run`(见 docker/Makefile)。
- 每个模块必须有八段式 README(背景/架构/流程/启动/验证/源码讲解/踩坑/最佳实践+面试题)。

## 模块索引(与 docs/ 章节一一映射,规划详见 docs/README.md)

| 模块 | 主题 | 案例数(目标) | 状态 |
|---|---|---|---|
| [e01-hello-flink](./e01-hello-flink/README.md) | 运行时 / 事件时间 / Kafka 端到端 | 3 | ✅ Phase 0 |
| [e02-time-window](./e02-time-window/README.md) | 窗口专题(乱序/会话/迟到旁路/自定义 Trigger/watermark 对齐) | 5 | ✅ Phase 1 |
| [e03-state](./e03-state/README.md) | 状态专题(五件套/Broadcast/TTL/Timer/RocksDB 实验) | 10 | ✅ Phase 1 |
| [e04-checkpoint](./e04-checkpoint/README.md) | Checkpoint/Savepoint(对齐 vs 非对齐/2PC/升级/混沌) | 4 | ✅ Phase 1 |
| [e05-sql](./e05-sql/README.md) | Flink SQL 专题(changelog/TVF/Join 家族/Top-N/去重/优化开关/UDF/EXPLAIN) | 10 | ✅ Phase 2 |
| [e06-table-api](./e06-table-api/README.md) | Table API 与 DataStream 双向桥接 | 8 | ✅ Phase 2 |
| [e07-connectors](./e07-connectors/README.md) | Kafka语义矩阵/JDBC/FileSink/自定义Source(FLIP-27)/自定义SinkV2/Redis/upsert-kafka | 8 | ✅ Phase 2 |
| [e08-cdc](./e08-cdc/README.md) | Flink CDC 3.6 YAML Pipeline 整库同步(PG→Kafka/Paimon,脱敏分流) | 4 | ✅ Phase 2 |
| [e09-lakehouse](./e09-lakehouse/README.md) | Paimon 主键表/changelog-producer/compaction/时间旅行 + Iceberg 对比 | 5 | ✅ Phase 2 |
| [e10-cep](./e10-cep/README.md) | CEP 专题(量词/连接语义/超时旁路/迭代条件/车联网告警预演) | 5 | ✅ Phase 2 |
| [e11-async-io](./e11-async-io/README.md) | Async I/O(有序/无序/重试降级/两级缓存维表富化) | 3 | ✅ Phase 2 |
| [e12-01](./e12-01-polling-vs-event/README.md) | 轮询 vs 事件驱动延迟对照(ai/01) | 1 | ✅ Phase 3 |
| [e12-02](./e12-02-event-bus/README.md) | 事件契约与消费者组隔离(ai/02) | 1 | ✅ Phase 3 |
| [e12-03](./e12-03-streaming-inference/README.md) | CREATE MODEL + ML_PREDICT SQL 脚本(ai/03,需本机 Ollama) | 2 SQL | ✅ Phase 3 |
| [e12-04](./e12-04-streaming-inference-vector/README.md) | 流式向量化 + VECTOR_SEARCH + 失效通道 SQL(ai/04/05,需 Milvus) | 4 SQL | ✅ Phase 3 |
| [e12-06](./e12-06-streaming-feature/README.md) | 实时特征双通道 + Redis 特征库(ai/06) | 1 | ✅ Phase 3 |
| [e12-07](./e12-07-agent-quickstart/README.md) | Flink Agents 快速上手(ai/07,**standalone 独立 pom,Preview 隔离**) | 1 | ✅ Phase 3 |
| [e12-08](./e12-08-streaming-memory/README.md) | 短期记忆读写(ai/08,standalone,Preview 隔离) | 1 | ✅ Phase 3 |
| [e12-15](./e12-15-observability/README.md) | Metrics + 结构化日志(ai/15) | 1 | ✅ Phase 3 |
| [e12-17](./e12-17-streaming-guardrail/README.md) | 流式护栏 Broadcast 热更(ai/17) | 1 | ✅ Phase 3 |
| [e12-18](./e12-18-streaming-cost-control/README.md) | Token 计量 + 预算熔断(ai/18) | 1 | ✅ Phase 3 |
| [e12-22](./e12-22-streaming-prompt/README.md) | Prompt 版本化确定性灰度(ai/22) | 1 | ✅ Phase 3 |

> 案例总数规划 ≥100,分布与验收口径见 ../PHASES.md。
