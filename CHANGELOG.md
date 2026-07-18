# Changelog

本文件遵循 Keep a Changelog 与语义化版本;每个 Phase 完成即发一个 minor 版本。

## [Unreleased]

### Added
- **projects/p02-realtime-reco Phase 5 单项目完成态（RECO-01–03）**：独立 compose profile `p02`；Kafka `reco.events` → Parse → Keyed State 会话特征 → Redis at-least-once 点查 → PG `reco_items` 规则 Top-K → 双写 `reco.results` + CH `reco_results`；验收 `make match`（CH 权威）
- 演练：`make loadtest` → `docs/baseline.md`（OrbStack arm64 实测）；`make drill-redis`（stop `fll-redis` 后 CH `feature_source=STATE_ONLY` 仍绿）
- 文档包：`docs/ARCHITECTURE.md`、`docs/adr/0001-dual-channel-features.md`、`docs/RESUME.md`；docs 模块 15-02 回填完成态
- **projects/p01-log-ai-platform Phase 4 单项目完成态（LOG-01–05）**：独立 compose profile `p01`；Kafka `logs.events` → Parse/Enrich/Rule →（可选）BudgetGate + Async Ollama 风险分级 → Guardrail → CH `log_results`；默认 `--ai.enabled=false` 零外部模型；验收双轨 `make verify` / `make verify-ai`
- 演练：`make loadtest` → `docs/baseline.md`（OrbStack arm64 实测）；`make drill-degrade`（AI off + 不可达 endpoint 后 verify 仍绿）
- 文档包：`docs/ARCHITECTURE.md`、`docs/adr/0001-ai-path-degradable.md`、`docs/RESUME.md`、`docs/DEGRADE-CHECKLIST.md`；docs 模块 15-01 回填完成态
- **projects/p03-vehicle-monitoring Phase 3 大盘与演练收官（VEH-05/06/07）**：旁路 `VehicleWindowMetricsJob` → CH `vehicle_window_metrics`；Grafana 双 DS 大盘 `p03-vehicle-overview`（`make verify-dashboard`）；`make loadtest` → `docs/baseline.md`；`make drill-watermark` 冻结 HEARTBEAT 停滞→恢复
- 文档包：`docs/ARCHITECTURE.md`、`docs/adr/0001-cep-broadcast-precompiled.md`、`docs/RESUME.md`；docs 模块 15-03 回填完成态
- **projects/p03-vehicle-monitoring Phase 2 模式库 + Broadcast**：静态三 CEP（`HARSH_THEN_FAULT` / `TRIPLE_HARSH` / `DTC_PAIR`）+ `vehicle.pattern.control` 控制面；出口门控按 `activePatterns` 过滤；CH `pattern_id` 列
- 造数三 scenario（`match-harsh-fault` / `match-triple-harsh` / `match-dtc-pair`，`match` 别名）与 `--publish-control`；Makefile `verify-switch`（TRUNCATE→control→gen→`PATTERN_ID` verify）
- 五元组文档 `docs/PATTERN-LIBRARY.md`；八段式 README 交叉引用与切换剧本
- **projects/p03-vehicle-monitoring Phase 1 告警链路样板**：独立 compose profile `p03`（topic + `vehicle_alerts` DDL）、CEP `HARSH_ACCEL→DTC` within(30s) + TIMEOUT Side Output、告警双写 Kafka/ClickHouse
- 造数 `scripts/gen_vehicle_events.py`（可判定 match 序列 + 尾心跳推进 watermark）与 `scripts/verify.sh`（ClickHouse MATCH count 为唯一权威出口；Kafka 仅诊断）

### Fixed
- docker Flink `FLINK_PROPERTIES`：MinIO S3a 固定 `SimpleAWSCredentialsProvider`，避免 InstanceProfile 探测导致作业长时间 `INITIALIZING`；去掉错误引号导致的 metrics port 解析失败
- `p03-init` ClickHouse DDL：CREATE 与 ALTER 分两次 HTTP POST（CH 24.8 HTTP 禁多语句，且无 `multiquery` 设置）

## [v0.4.0-phase3] - 2026-07-06

### Added
- **ai/ 《Flink AI Engineering》全书 24 章成文**(chapters/,约 1900 行):第 I 部基础设施(为什么需要 Streaming/事件总线/ML_PREDICT/向量化/Streaming RAG/特征工程)、第 II 部 Agent 运行时(Agents 0.3 架构与 Java 上手/三层记忆/Durable Execution+Reconciler/MCP 接入/Workflow 编排边界/多 Agent 协作/Flink×LangGraph/知识图谱)、第 III 部生产化(可观测性/全链路 Trace/护栏/成本熔断/AI 网关/语义缓存/在线评测/Prompt 灰度/样本管线/参考架构收官含 AIDO 映射表)
- e12 系列 Demo × 11:零依赖 7 个进主 Maven 构建(e12-01 轮询对照/e12-02 事件契约/e12-06 特征双通道/e12-15 可观测性/e12-17 护栏热更/e12-18 成本熔断/e12-22 Prompt 确定性灰度);SQL 脚本 2 个(e12-03 ML_PREDICT、e12-04 向量化+VECTOR_SEARCH+失效通道,均注明版本演进风险);Flink Agents Preview 2 个(e12-07/e12-08,standalone 独立 pom 隔离 Preview 依赖)
- docker:Milvus v2.6.19 ai-profile(etcd+独立minio+standalone,`make up-ai` 按需启动);根 README 版本矩阵新增 Milvus/Ollama 条目
- docs/11-ecosystem 生态协同全文:框架选型矩阵、Flink×Ray 分工、流式数据库对比(RisingWave 厂商基准已标注立场)、**StateFun 停运确认(2026-01 社区决定,来源已注)**、Remote Shuffle
- 父 pom:e12 零依赖模块 ×7 注册、flink-metrics-dropwizard 依赖管理、flink.agents.version=0.3.0 SSOT 注释

### Notes
- Agents standalone 模块(e12-07/08)未做编译验证(沙箱无 Maven Central);Ollama/Milvus 端到端链路留本机验证——各 README 已给出首次运行核对清单与降级路径
- Flink Agents 0.3.0 Maven 坐标已确认存在于 Central(flink-agents-api / flink-agents-ide-support / integrations-chat-models-ollama)

## [v0.3.0-phase2] - 2026-07-05

### Added
- docs 模块 05–10 全文:Flink SQL(changelog三形态/窗口TVF/Join五家族/去重Top-N/优化开关/Catalog/Materialized Table/SQL AI函数)、Table API 桥接(双向转换/表达式DSL/Catalog编程)、连接器深度(Kafka语义矩阵/JDBC双面/FileSink物理层/自定义FLIP-27 Source/自定义SinkV2/upsert-kafka)、CDC(增量快照框架/YAML Pipeline三段式/Schema演进治理)、Lakehouse(Paimon主键表与LSM/changelog-producer/Compaction与TimeTravel/Paimon vs Iceberg)、CEP(NFA心智模型/量词与连接语义/超时旁路/IterativeCondition/性能红线)
- e05-sql × 10:changelog基础观察、TUMBLE/HOP/CUMULATE三连、Top-N、去重、Interval/Regular/Temporal Join、mini-batch优化、UDF/UDTF、EXPLAIN与Hints
- e06-table-api × 8:DataStream↔Table双向桥接(带事件时间)、表达式DSL、SQL/Table混编、fromChangelogStream、Catalog编程接口、call()调UDF、桥接往返时间语义验证
- e07-connectors × 8:Kafka三级投递语义一键切换、JDBC upsert+Lookup Join、Kafka key/headers/自定义分区器、FileSink→MinIO滚动策略、自定义FLIP-27 Source(四部件解剖)、自定义SinkV2(ClickHouse HTTP)、Redis攒批写(jedis pipeline)、upsert-kafka
- e08-cdc:PG→Kafka/Paimon整库同步YAML Pipeline、声明式脱敏分流、Postgres CDC DataStream编程接口、增量快照框架讲解
- e09-lakehouse:Paimon主键表/changelog-producer/compaction/时间旅行/Iceberg对照(SQL脚本形态,不进Maven modules)
- e10-cep × 5:times连续量词、next vs followedBy连接语义对照、超时半成品旁路(挽单场景)、IterativeCondition相对条件、车联网告警模式雏形
- e11-async-io × 3:有序vs无序等待、超时重试降级三件套、两级维表缓存(LRU+异步回填)
- playground P04-P20:Top-N/去重/三种Join/EXPLAIN/Hints/UDF/桥接/Catalog/upsert-kafka/Lookup Join/CDC读取/Paimon建表流读/MATCH_RECOGNIZE/CDC到湖仓综合演练
- templates/job-datastream、templates/job-sql:固化交付三件套纪律的作业脚手架
- scripts/fetch_lakehouse_jars.sh:Paimon/Iceberg jar 下载脚本(离线环境降级提示)
- 父 pom:新增 e05/e06/e07/e08/e10/e11 模块与 jdbc/postgres-cdc/jedis/cep/files 依赖

### Notes
- e08/e09 的集群步骤(YAML Pipeline提交、Paimon/Iceberg SQL)已给出完整命令,受限于沙箱无 Maven Central 与无法起多容器集群,未在本会话内实际跑通,验证请在本机 docker 环境执行
- 全部代码按 Flink 2.x/CDC 3.6/Paimon 生态断代核对(见 docs/00-landscape 调研来源)

## [v0.2.0-phase1] - 2026-07-05

### Added
- docs 模块 01–04 全文:Runtime 内核(架构/四层图/Slot·Chain/内存/网络反压/序列化/部署 HA/调度器)、时间与窗口(语义决策树/watermark 生成传播对齐空闲/窗口生命周期/Trigger·迟到三分层/Timer)、状态(五件套/Operator·Broadcast/HashMap·RocksDB·ForSt 选型/RocksDB 调优/TTL 治理/Side Output/序列化演进)、容错(checkpoint 全链路/savepoint 与 uid 契约/State Processor API/端到端 2PC)
- examples/common 基座模块:Event POJO + Labs 数据源工具(FLIP-27 datagen 突发式/逻辑时钟两种无界源)
- e02-time-window × 5:乱序补偿对照、会话窗口、迟到三分层旁路、自定义提前触发 Trigger、多流 watermark 对齐(FLIP-182)
- e03-state × 10:ValueState/ListState/MapState/AggregatingState、Operator State 攒批、State TTL、Broadcast 动态规则、Side Output 分流死信、Timer 超时检测、RocksDB 增量 checkpoint 本地观察
- e04-checkpoint × 4(5 类文件):对齐 vs 非对齐反压对照(本地 WebUI)、Kafka 端到端 EXACTLY_ONCE 2PC、Savepoint 升级 V1→V2、故障注入与恢复混沌实验
- scripts/qa_check.sh:compose 校验、违禁词扫描、Markdown 断链检查、案例计数、可选 mvn 编译
- 父 pom:新增 common/e02/e03/e04 模块与 flink-connector-datagen 依赖;local profile 增补 RocksDB 后端与本地 WebUI

### Notes
- 全部代码按 Flink 2.x API 断代核对(OpenContext、core.execution.CheckpointingMode、Duration 窗口、execution.checkpointing.* 键、FLIP-27 源);沙箱无 Maven Central,编译验证须在本机执行

## [v0.1.0-phase0] - 2026-07-04

### Added
- 仓库全目录骨架与工程约定(根 README 第 5 节)
- Level 1–10 完整学习路线(roadmap/)
- docker/ 一键环境:Kafka(KRaft)+ Flink 2.2.1(java21,checkpoint 落 MinIO S3)+ PG/Redis/ClickHouse/MinIO/Prometheus/Grafana/Kafka UI,含 Makefile、init.sh、故障排查
- examples/ Maven 多模块基座(版本 SSOT、shade、local profile)与 e01 三作业:本地事件时间窗口 / Kafka 端到端 PV-UV / 纯 SQL datagen 窗口
- scripts/gen_events.py(uv 单文件,乱序与倾斜可控)
- docs/README.md 章节 SSOT 索引;docs/00-landscape 2026 技术版图(含 ADR-001 版本决策与来源清单)
- ai/README.md《Flink AI Engineering》24 章全书大纲与技术底座简报
- playground 首批 SQL 练习、datasets 规范、benchmark 方法论、monitoring 指标手册、production 蓝图、templates 规范
- cheatsheet、interview 首批 30 题、best-practice 首批 12 条军规
- PHASES.md 阶段计划与 Claude Code/open-gsd 接力协议

### Notes
- 主线锁定 Flink 2.2.1(而非 2.3.0)的决策记录见 docs/00-landscape ADR-001
