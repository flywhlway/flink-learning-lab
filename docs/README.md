# docs/ · 教材总索引(SSOT)

> 本文件是全仓库章节编号、命名与交叉引用的**唯一事实来源**。任何新增文档必须先在此登记编号。
> 状态图例:✅ 已交付 · 🚧 当前 Phase 进行中 · 📋 已规划(所属 Phase 见括号)

## 模块 00 · 技术版图(持续更新)

| 编号 | 章节 | 状态 |
|---|---|---|
| 00-01 | [2026 年 Flink 技术版图与版本决策](./00-landscape/01-flink-2026-landscape.md) | ✅ |
| 00-02 | Flink 2.3 新特性追踪(changelog 算子 / Materialized Table / 原生 S3 FS) | 📋(随 2.3 生态就绪更新) |

## 模块 01 · Runtime 内核 ✅([全文](./01-runtime/README.md))

01-01 架构总览:JobManager(Dispatcher/ResourceManager/JobMaster)与 TaskManager · 01-02 四层图转换:StreamGraph→JobGraph→ExecutionGraph→物理执行 · 01-03 Task/Slot/SlotSharing/OperatorChain · 01-04 内存模型与 taskmanager.memory.* 全配置 · 01-05 网络栈:Credit-based 流控与反压 · 01-06 序列化体系:PojoSerializer/Kryo/自定义 TypeInfo · 01-07 部署形态与 HA · 01-08 调度器:Default/Adaptive/AdaptiveBatch 与细粒度资源管理

## 模块 02 · 时间与窗口 ✅([全文](./02-time-window/README.md),配套 e02 × 5 案例)

02-01 时间语义决策树 · 02-02 Watermark 生成/传播/对齐/空闲 · 02-03 窗口四类型与生命周期 · 02-04 Trigger/Evictor/allowedLateness/迟到旁路 · 02-05 Timer 与 ProcessFunction 精讲

## 模块 03 · 状态 ✅([全文](./03-state/README.md),配套 e03 × 10 案例)

03-01 Keyed State 五件套 · 03-02 Operator/Broadcast State · 03-03 State Backend 选型:HashMap vs RocksDB vs ForSt(存算分离) · 03-04 RocksDB 深度调优 · 03-05 TTL 与状态膨胀治理 · 03-06 Side Output 模式集 · 03-07 状态序列化与 schema 演进

## 模块 04 · 容错 ✅([全文](./04-checkpoint/README.md),配套 e04 × 4 案例)

04-01 Checkpoint 全链路(Barrier/对齐/非对齐/增量) · 04-02 Savepoint 与作业演进(uid 纪律) · 04-03 State Processor API 离线读写状态 · 04-04 端到端一致性与两阶段提交

## 模块 05 · Flink SQL ✅([全文](./05-sql/README.md),配套 e05 × 10 案例)

05-01 动态表与 changelog(+I/-U/+U/-D) · 05-02 窗口 TVF 家族 · 05-03 Join 家族与状态代价(Regular/Interval/Temporal/Lookup/Window/Delta) · 05-04 去重与 Top-N · 05-05 优化开关(mini-batch/local-global/two-phase) · 05-06 Catalog 体系 · 05-07 Materialized Table 与流批一体 · 05-08 SQL AI 函数(CREATE MODEL/ML_PREDICT/VECTOR_SEARCH,与 ai/ 模块互链)

## 模块 06 · Table API 与 DataStream 互转 ✅([全文](./06-table-api/README.md),配套 e06 × 8 案例)

06-01 三层 API 分工 · 06-02 双向桥接(fromDataStream/fromChangelogStream/toDataStream/toChangelogStream) · 06-03 表达式 DSL 与函数 · 06-04 Catalog 编程接口

## 模块 07 · 连接器 ✅([全文](./07-connectors/README.md),配套 e07 × 8 案例)

07-01 Kafka 语义矩阵(NONE/AT_LEAST_ONCE/EXACTLY_ONCE) · 07-02 JDBC 双面(Sink upsert / Source Lookup) · 07-03 FileSink 物理层与滚动策略 · 07-04 自定义 Source(FLIP-27 四部件) · 07-05 自定义 Sink(SinkV2 语义台阶,含 ClickHouse/Redis 攒批案例) · 07-06 upsert-kafka

## 模块 08 · CDC 与数据集成 ✅([全文](./08-cdc/README.md),配套 e08 × 4 案例)

08-01 CDC 心智模型(从抽取到整库同步治理) · 08-02 增量快照框架(无锁全量+增量归并) · 08-03 YAML Pipeline 三段式(source/transform/route+sink) · 08-04 Schema 演进与治理红线

## 模块 09 · Lakehouse ✅([全文](./09-lakehouse/README.md),配套 e09 × 5 SQL 脚本)

09-01 湖仓表格式核心抽象 · 09-02 Paimon 主键表与 LSM · 09-03 changelog-producer 信息量旋钮 · 09-04 Compaction 与 Time Travel · 09-05 Paimon vs Iceberg 选型 · 09-06 与 Materialized Table 的关系(Hudi/Fluss 对比留待后续版本按需扩展)

## 模块 10 · CEP ✅([全文](./10-cep/README.md),配套 e10 × 5 案例)

10-01 NFA 心智模型 · 10-02 量词与连接语义(next/followedBy/followedByAny) · 10-03 超时半成品与 AfterMatchSkipStrategy · 10-04 IterativeCondition 相对条件 · 10-05 性能红线与动态化路线(车联网告警模式库雏形,案例三预演)

## 模块 11 · 生态协同 ✅([全文](./11-ecosystem/README.md))

11-01 流处理框架选型矩阵(Flink/Kafka Streams/Spark/Beam/ksqlDB) · 11-02 Flink × Ray 训练-推理分工 · 11-03 Flink × 流式数据库(RisingWave/Materialize,含信源立场标注) · 11-04 StateFun 已确认停运(2026-01 社区决定,新项目零选型) · 11-05 Remote Shuffle 与批处理增强

## 模块 12 · AI 专项 ✅(全书独立成册:[ai/](../ai/README.md),24 章 + e12 系列 Demo)

**独立成书,见 [ai/README.md](../ai/README.md)**(《Flink AI Engineering》全书大纲已交付)。

## 模块 13 · 性能与压测 ✅([索引](./13-performance/README.md),权威报告 [benchmark/baseline.md](../benchmark/baseline.md))

13-01 压测方法论与单变量原则 · 13-02 裁剪矩阵(作业轴 e01-J2 / e10 `C5VehicleDtcPatternJob` / p03 `VehicleAlertJob` + 负载 1k/5k eps 必跑) · 13-03 仓库级 [`benchmark/baseline.md`](../benchmark/baseline.md)（OrbStack arm64 / compose Flink 实测）· 入口脚本 `make -C benchmark matrix`；项目级 baseline 不替代本报告。

## 模块 14 · 生产化 ✅([索引](./14-production/README.md),权威路径 [production/](../production/) · [monitoring/](../monitoring/) · [best-practice/](../best-practice/))

14-01 K8s Operator 1.15（CRD/升级模式/Blue-Green 状态机硬门禁/Autoscaler 附录）· 14-02 可观测（Prometheus + 仓库级三块 Grafana JSON；Loki/OTel 可选增强非硬门禁）· 14-03 CI/CD 与 GitOps（Helm/唯一 Argo CD/GitHub Actions；禁止并行 Flux）· 14-04 多租户与成本治理 · 14-05 安全与合规视角 · 落地清单在 `production/`，规范正文在 `best-practice/`，题库 ≥150 在 `interview/`。

## 模块 15 · 企业实战三案例(P4)

见 PHASES.md Phase 4 定义;每个案例独立目录:`projects/p01-log-ai-platform`、`projects/p02-realtime-reco`、`projects/p03-vehicle-monitoring`。

| 编号 | 项目 | 状态 |
|---|---|---|
| 15-03 | [p03 车联网监控 · P4 单项目完成态](../projects/p03-vehicle-monitoring/README.md)（compose profile `p03` + 三 CEP + Broadcast + 旁路窗口大盘 + 压测/WM 演练） | ✅ 完成：Grafana 双 DS JSON（`p03-vehicle-overview`）+ [baseline.md](../projects/p03-vehicle-monitoring/docs/baseline.md) + [ADR-0001](../projects/p03-vehicle-monitoring/docs/adr/0001-cep-broadcast-precompiled.md) + [RESUME](../projects/p03-vehicle-monitoring/docs/RESUME.md) + [ARCHITECTURE](../projects/p03-vehicle-monitoring/docs/ARCHITECTURE.md) 可打开；CH 仍为 CEP 权威 |
| 15-01 | [p01 日志 AI 平台 · P4 单项目完成态](../projects/p01-log-ai-platform/README.md)（compose profile `p01` + 规则路径默认 AI off + 可选 Async Ollama + BudgetGate/Guardrail + 压测/降级演练） | ✅ 完成：`make verify` / `make verify-ai`（可选）+ [baseline.md](../projects/p01-log-ai-platform/docs/baseline.md) + [ADR-0001](../projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md) + [RESUME](../projects/p01-log-ai-platform/docs/RESUME.md) + [ARCHITECTURE](../projects/p01-log-ai-platform/docs/ARCHITECTURE.md) 可打开；CH 仍为规则/AI 权威；主构建零硬依赖 Preview/外部模型 |
| 15-02 | [p02 实时推荐 · P4 单项目完成态](../projects/p02-realtime-reco/README.md)（compose profile `p02` + Keyed State/Redis 双通道特征 + PG 目录规则 Top-K + 压测/Redis 降级演练） | ✅ 完成：`make match` / `make drill-redis` / `make loadtest` + [baseline.md](../projects/p02-realtime-reco/docs/baseline.md) + [ADR-0001](../projects/p02-realtime-reco/docs/adr/0001-dual-channel-features.md) + [RESUME](../projects/p02-realtime-reco/docs/RESUME.md) + [ARCHITECTURE](../projects/p02-realtime-reco/docs/ARCHITECTURE.md) 可打开；CH 仍为推荐权威；Redis 写 at-least-once，读失败 STATE_ONLY |

---

### 交叉引用规则

- 文档引用示例统一写相对路径 `../../examples/eNN-.../README.md#锚点`;示例回链教材写 `docs/NN-模块/章节`。
- 版本号一律指向根 README 版本矩阵,文档正文不落具体版本数字(00-landscape 除外)。
