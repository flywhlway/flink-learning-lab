# examples/ · 可运行 Demo 工程(Maven 多模块)

统一规则:

- 模块命名 `eNN-<主题>`;所有版本与插件由父 `pom.xml` 统一管理(SSOT),子模块**禁止**声明任何版本号。
- Flink 核心 `provided`;连接器与业务依赖打入 shaded 作业 jar。
- 本地运行:`mvn -Plocal compile exec:java -pl <module> -Dexec.mainClass=<Job>`;集群提交:构建后经 `docker/jobs/` 卷 + `flink run`(见 docker/Makefile)。
- 每个模块必须有八段式 README(背景/架构/流程/启动/验证/源码讲解/踩坑/最佳实践+面试题)。

## 模块索引(与 docs/ 章节一一映射,规划详见 docs/README.md)

| 模块 | 主题 | 案例数(目标) | 状态 |
|---|---|---|---|
| [e01-hello-flink](./e01-hello-flink/README.md) | 运行时 / 事件时间 / Kafka 端到端 | 5 | ✅ Phase 0+7 |
| [e02-time-window](./e02-time-window/README.md) | 窗口专题(乱序/会话/迟到旁路/自定义 Trigger/watermark 对齐/滑动/计数批) | 7 | ✅ Phase 1+7 |
| [e03-state](./e03-state/README.md) | 状态专题(五件套/Broadcast/TTL/Timer/RocksDB 实验) | 10 | ✅ Phase 1 |
| [e04-checkpoint](./e04-checkpoint/README.md) | Checkpoint/Savepoint(对齐 vs 非对齐/2PC/升级/混沌/计数/uid) | 6 | ✅ Phase 1+7 |
| [e05-sql](./e05-sql/README.md) | Flink SQL 专题(changelog/TVF/Join 家族/Top-N/去重/优化开关/UDF/EXPLAIN) | 10 | ✅ Phase 2 |
| [e06-table-api](./e06-table-api/README.md) | Table API 与 DataStream 双向桥接 | 8 | ✅ Phase 2 |
| [e07-connectors](./e07-connectors/README.md) | Kafka语义矩阵/JDBC/FileSink/自定义Source(FLIP-27)/自定义SinkV2/Redis/upsert-kafka | 8 | ✅ Phase 2 |
| [e08-cdc](./e08-cdc/README.md) | Flink CDC 3.6 YAML + DataStream changelog 教学(含零依赖 C5–C7) | 7 | ✅ Phase 2+7 |
| [e09-lakehouse](./e09-lakehouse/README.md) | Paimon 主键表/changelog-producer/compaction/时间旅行 + Iceberg 对比 | 5 | ✅ Phase 2 |
| [e10-cep](./e10-cep/README.md) | CEP 专题(量词/连接语义/超时旁路/迭代条件/车联网/漏斗/尖刺) | 7 | ✅ Phase 2+7 |
| [e11-async-io](./e11-async-io/README.md) | Async I/O(有序/无序/重试/缓存/预算/熔断/预刷新) | 6 | ✅ Phase 2+7 |
| [e12-01](./e12-01-polling-vs-event/README.md) | 轮询 vs 事件驱动延迟对照(ai/01) | 2 | ✅ Phase 3+7 |
| [e12-02](./e12-02-event-bus/README.md) | 事件契约与消费者组隔离(ai/02) | 1 | ✅ Phase 3 |
| [e12-03](./e12-03-streaming-inference/README.md) | CREATE MODEL + ML_PREDICT SQL 脚本(ai/03,需本机 Ollama) | 2 SQL | ✅ Phase 3 |
| [e12-04](./e12-04-streaming-inference-vector/README.md) | 流式向量化 + VECTOR_SEARCH + 失效通道 SQL(ai/04/05,需 Milvus) | 4 SQL | ✅ Phase 3 |
| [e12-06](./e12-06-streaming-feature/README.md) | 实时特征双通道 + Redis 特征库(ai/06) | 2 | ✅ Phase 3+7 |
| [e12-07](./e12-07-agent-quickstart/README.md) | Flink Agents 快速上手(ai/07,**standalone 独立 pom,Preview 隔离**) | 1 | ✅ Phase 3 |
| [e12-08](./e12-08-streaming-memory/README.md) | 短期记忆读写(ai/08,standalone,Preview 隔离) | 1 | ✅ Phase 3 |
| [e12-15](./e12-15-observability/README.md) | Metrics + 结构化日志(ai/15) | 2 | ✅ Phase 3+7 |
| [e12-17](./e12-17-streaming-guardrail/README.md) | 流式护栏 Broadcast 热更(ai/17) | 2 | ✅ Phase 3+7 |
| [e12-18](./e12-18-streaming-cost-control/README.md) | Token 计量 + 预算熔断(ai/18) | 2 | ✅ Phase 3+7 |
| [e12-22](./e12-22-streaming-prompt/README.md) | Prompt 版本化确定性灰度(ai/22) | 2 | ✅ Phase 3+7 |
| [e12-05](./e12-05-streaming-rag-lite/README.md) | Streaming RAG Lite 片段索引(ai/05,零依赖) | 2 | ✅ Phase 7 |
| [e12-09](./e12-09-streaming-tool-call/README.md) | Tool Call 幂等键+副作用侧输出(ai/09) | 2 | ✅ Phase 7 |
| [e12-11](./e12-11-streaming-workflow/README.md) | Workflow FSM ProcessFunction(ai/11) | 1 | ✅ Phase 7 |
| [e12-12](./e12-12-multi-agent-topology/README.md) | Multi-Agent 双流 connect(ai/12) | 1 | ✅ Phase 7 |
| [e12-13](./e12-13-langgraph-mock/README.md) | LangGraph Mock AsyncIO 降级(ai/13) | 1 | ✅ Phase 7 |
| [e12-14](./e12-14-knowledge-graph-events/README.md) | KG 三元组 MapState(ai/14) | 1 | ✅ Phase 7 |
| [e12-16](./e12-16-trace-propagation/README.md) | TraceId 跨算子传播(ai/16) | 1 | ✅ Phase 7 |
| [e12-19](./e12-19-ai-gateway-route/README.md) | AI Gateway Broadcast 路由(ai/19) | 1 | ✅ Phase 7 |
| [e12-20](./e12-20-embedding-cache/README.md) | Embedding Cache LRU(ai/20) | 1 | ✅ Phase 7 |
| [e12-21](./e12-21-streaming-evaluation/README.md) | Streaming Evaluation 窗口指标(ai/21) | 1 | ✅ Phase 7 |
| [e12-23](./e12-23-online-learning-sample/README.md) | Online Learning 样本侧输出(ai/23) | 1 | ✅ Phase 7 |

> 案例总数规划 ≥100,分布与验收口径见 ../PHASES.md。
