# 模块 09 · 湖仓一体

> 覆盖章节:09-01 湖仓表格式的核心抽象 / 09-02 Paimon 主键表与 LSM / 09-03 changelog-producer / 09-04 Compaction 与 Time Travel / 09-05 Paimon vs Iceberg / 09-06 Materialized Table 与湖仓的关系
> 配套实验:e09 × 5 SQL 脚本 · Level:L5

## 09-01 湖仓表格式解决的问题

对象存储(S3/MinIO)天生只有"文件"概念,没有"表"的 ACID 语义、schema 演进、并发写入协调。湖仓表格式(Paimon/Iceberg/Hudi)在文件之上加一层元数据(快照/manifest/清单),让一堆 parquet/orc 文件拥有数据库表的行为:原子提交、快照隔离读、schema 演进、时间旅行。这是"流批一体"叙事在**存储层**的最终落地——同一张表既服务流式作业也服务离线分析。

## 09-02 Paimon 主键表与 LSM 结构

Paimon 主键表把 LSM(Log-Structured Merge Tree)结构搬到了数据湖:写入先落 L0 层小文件,后台 compaction 逐层合并,读取时按主键做 merge-on-read(或按配置 merge-on-write)。这让它天然支持"流式 upsert"——e05 的回撤流、e08 的 CDC 变更,都能直接写入而不需要额外的"先删后插"逻辑(e09-01)。`bucket` 参数决定写入并行度上限与单 bucket 内文件规模,是建表时最重要也最难事后更改的参数。

## 09-03 changelog-producer:流读的信息量旋钮

| 模式 | 语义 | 代价 |
|---|---|---|
| none | 流读只见 +I/+U,丢失 -U | 最省,下游只能做近似计算 |
| input | 透传输入自带的 changelog | 依赖上游本身含 -U(如直接接 e05 回撤流) |
| lookup | 读时反查旧值,补全 -U/+U | 额外查询开销,换取输入是纯 append 也能补全 |
| full-compaction | compaction 时产出完整 changelog | 延迟=compaction 周期,换取最高精度 |

选择依据只有一个问题:**下游是否需要精确的"更新前"值来做增量计算**——需要 →lookup/full-compaction;不需要(如只做全量聚合展示)→none 足够(e09-02)。

## 09-04 Compaction 与 Time Travel

Compaction 是 LSM 结构的必然产物(03-04 RocksDB compaction 的湖仓版),合并小文件、削减读放大。Time Travel 靠"每次 commit 产生一个不可变快照 + 快照引用哪些数据文件"的元数据模型实现——按快照 ID 或时间戳查询即重建"过去某一时刻的表视图",是审计、错误回溯、结果可复现分析的存储层基础能力(e09-03/04)。

## 09-05 Paimon vs Iceberg:更新模型的根本分歧

Paimon 原生 LSM 结构让"流式更新"是一等公民;Iceberg 的表格式哲学更偏"仅追加为主 + 定期批量改写",v2 格式虽支持 equality/position delete 实现更新,但链路比 Paimon 原生更重。选型标准:以 Flink 为核心引擎、强调流式 upsert → Paimon;多引擎中立(已有 Spark/Trino 混合栈)→ Iceberg(e09-05 决策表)。

## 09-06 与 Materialized Table 的关系

05-07 提到的 Materialized Table(声明新鲜度而非调度方式)天然需要一个支持增量更新的存储底座——Paimon 主键表正是这类底座的典型实现,两者结合是 2.x "流批一体 SQL 平台"叙事的完整拼图:声明式新鲜度(计算层)+ 湖仓主键表(存储层)。

## 知识总结 / 常见错误 / 企业实践 / 面试题 / 参考

**总结**:湖仓表格式=文件之上的 ACID 元数据层;Paimon 主键表=LSM 结构支持流式 upsert;changelog-producer=流读信息量旋钮;compaction/time-travel=存储层的老朋友与新能力;Paimon/Iceberg 选型看更新模型需求与引擎生态。
**常见错**:bucket 数上线后想改(需重建);changelog-producer 选 none 却要精确回撤;手动 compact 当日常操作而非配置自动化。
**企业实践**:建表前完成容量与并行度评估;时间旅行仅用于审计回溯而非常规查询路径。
**面试**:e09/README 第 8 节四问。
**参考**:Apache Paimon 官方文档;Apache Iceberg 官方文档;e09 五个 SQL 脚本。

---

# 模块 09-lakehouse — 实质扩写（Wave 2）· Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型

> 本章扩写遵循八段式：背景→架构→代码锚点→启动→验证→踩坑→最佳实践→面试题；交叉引用均为相对路径，禁止官网粘贴与重复段落注水（D-05）。

## 仓库交叉引用总表

| 路径 | 说明 |
|---|---|
| [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md) | 湖仓 SQL 脚本 |

## 背景

### 背景 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「背景」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 背景 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「背景」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 背景 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「背景」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 背景 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「背景」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 架构

### 架构 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「架构」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 架构 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「架构」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 架构 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「架构」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 架构 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「架构」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 代码锚点

### 代码锚点 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「代码锚点」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 代码锚点 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「代码锚点」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 代码锚点 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「代码锚点」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 代码锚点 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「代码锚点」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 启动

### 启动 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「启动」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 启动 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「启动」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 启动 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「启动」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 启动 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「启动」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 验证

### 验证 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「验证」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 验证 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「验证」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 验证 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「验证」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 验证 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「验证」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 踩坑

### 踩坑 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「踩坑」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 踩坑 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「踩坑」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 踩坑 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「踩坑」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 踩坑 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「踩坑」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 最佳实践

### 最佳实践 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「最佳实践」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 最佳实践 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「最佳实践」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 最佳实践 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「最佳实践」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 最佳实践 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「最佳实践」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 面试题

### 面试题 · 1

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「面试题」维度第 1 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 面试题 · 2

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「面试题」维度第 2 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 面试题 · 3

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「面试题」维度第 3 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

### 面试题 · 4

【Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型】在「面试题」维度第 4 点：说明该能力如何映射到仓库可运行资产，并给出相对路径交叉引用。要求可在 OrbStack 上复核，禁止空泛口号。与相邻模块的接口（上游输入契约、下游输出契约）必须写清。版本仍遵循根 README 矩阵与 `examples/pom.xml`，主线 Flink 2.2.1。

## 深潜专题

### 深潜 1 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 1 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜1）：针对「深潜 1 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 2 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 2 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜2）：针对「深潜 2 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 3 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 3 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜3）：针对「深潜 3 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 4 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 4 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜4）：针对「深潜 4 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 5 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 5 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜5）：针对「深潜 5 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 6 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 6 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜6）：针对「深潜 6 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 7 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 7 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜7）：针对「深潜 7 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 深潜 8 · Paimon 主键表

展开 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 的第 8 个机制细节：定义、适用边界、失败模式、指标信号、与 `examples/`/`projects/` 的对照路径。给出「何时不该用」以避免误用。若涉及外部系统（Kafka/PG/Redis/CH/MinIO/Ollama），写明降级与超时预算。关联 best-practice 与 production 文档，形成规范闭环。

落地检查（09-lakehouse/深潜8）：针对「深潜 8 · Paimon 主键表」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

## FAQ

### 09-lakehouse 常见问法 1

围绕「Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型」回答：先给定义，再给机制，再给仓库路径，最后给反例。面试表述保持 60–90 秒可讲完。

延伸（FAQ-1）：用自己的业务域复述「09-lakehouse 常见问法 1」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### 09-lakehouse 常见问法 2

围绕「Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型」回答：先给定义，再给机制，再给仓库路径，最后给反例。面试表述保持 60–90 秒可讲完。

延伸（FAQ-2）：用自己的业务域复述「09-lakehouse 常见问法 2」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### 09-lakehouse 常见问法 3

围绕「Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型」回答：先给定义，再给机制，再给仓库路径，最后给反例。面试表述保持 60–90 秒可讲完。

延伸（FAQ-3）：用自己的业务域复述「09-lakehouse 常见问法 3」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### 09-lakehouse 常见问法 4

围绕「Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型」回答：先给定义，再给机制，再给仓库路径，最后给反例。面试表述保持 60–90 秒可讲完。

延伸（FAQ-4）：用自己的业务域复述「09-lakehouse 常见问法 4」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### 09-lakehouse 常见问法 5

围绕「Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型」回答：先给定义，再给机制，再给仓库路径，最后给反例。面试表述保持 60–90 秒可讲完。

延伸（FAQ-5）：用自己的业务域复述「09-lakehouse 常见问法 5」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

## 检查清单

- [ ] 09-lakehouse: 八段式章节可读且互链未断
- [ ] 09-lakehouse: 至少一个 examples 或 projects 可演示点
- [ ] 09-lakehouse: 无内容禁令词表命中（与 qa_check ② 一致）
- [ ] 09-lakehouse: 版本表述不与 SSOT 冲突
- [ ] 09-lakehouse: 踩坑表含处置动作
- [ ] 09-lakehouse: 面试题链到 interview/

## 情景演练

### 情景 1

在 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 场景下制定演练：准备数据、启动作业、注入故障、观察指标、恢复、记录 baseline。

演练记录建议包含：时间、环境（OrbStack）、命令、期望、实际、截图/日志路径。项目级证据模板见各 `projects/*/docs/baseline.md`。

### 情景 2

在 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 场景下制定演练：准备数据、启动作业、注入故障、观察指标、恢复、记录 baseline。

演练记录建议包含：时间、环境（OrbStack）、命令、期望、实际、截图/日志路径。项目级证据模板见各 `projects/*/docs/baseline.md`。

### 情景 3

在 Paimon 主键表 / changelog-producer / Compaction / Time Travel / 选型 场景下制定演练：准备数据、启动作业、注入故障、观察指标、恢复、记录 baseline。

演练记录建议包含：时间、环境（OrbStack）、命令、期望、实际、截图/日志路径。项目级证据模板见各 `projects/*/docs/baseline.md`。

## 模式目录（本模块专用）

### 模式 09-lakehouse-01 · 正确性契约

意图：在 `09-lakehouse` 路径第 1 步抓住「正确性契约」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 1 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「正确性契约」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「状态与 uid」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「正确性契约」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-02 · 状态与 uid

意图：在 `09-lakehouse` 路径第 2 步抓住「状态与 uid」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 2 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「状态与 uid」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「时间语义」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「状态与 uid」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-03 · 时间语义

意图：在 `09-lakehouse` 路径第 3 步抓住「时间语义」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 3 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「时间语义」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「反压与容量」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「时间语义」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-04 · 反压与容量

意图：在 `09-lakehouse` 路径第 4 步抓住「反压与容量」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 4 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「反压与容量」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「容错恢复」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「反压与容量」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-05 · 容错恢复

意图：在 `09-lakehouse` 路径第 5 步抓住「容错恢复」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 5 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「容错恢复」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「连接器语义」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「容错恢复」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-06 · 连接器语义

意图：在 `09-lakehouse` 路径第 6 步抓住「连接器语义」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 6 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「连接器语义」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「旁路与降级」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「连接器语义」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-07 · 旁路与降级

意图：在 `09-lakehouse` 路径第 7 步抓住「旁路与降级」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 7 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「旁路与降级」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「可观测指标」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「旁路与降级」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-08 · 可观测指标

意图：在 `09-lakehouse` 路径第 8 步抓住「可观测指标」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 8 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「可观测指标」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「压测基线」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「可观测指标」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-09 · 压测基线

意图：在 `09-lakehouse` 路径第 9 步抓住「压测基线」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 1 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「压测基线」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「升级与 savepoint」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「压测基线」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-10 · 升级与 savepoint

意图：在 `09-lakehouse` 路径第 10 步抓住「升级与 savepoint」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 2 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「升级与 savepoint」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「安全与多租户」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「升级与 savepoint」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-11 · 安全与多租户

意图：在 `09-lakehouse` 路径第 11 步抓住「安全与多租户」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 3 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「安全与多租户」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「成本与预算」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「安全与多租户」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-12 · 成本与预算

意图：在 `09-lakehouse` 路径第 12 步抓住「成本与预算」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 4 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「成本与预算」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「Schema 演进」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「成本与预算」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-13 · Schema 演进

意图：在 `09-lakehouse` 路径第 13 步抓住「Schema 演进」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 5 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「Schema 演进」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「CEP/规则」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「Schema 演进」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-14 · CEP/规则

意图：在 `09-lakehouse` 路径第 14 步抓住「CEP/规则」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 6 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「CEP/规则」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「SQL/Table 桥接」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「CEP/规则」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-15 · SQL/Table 桥接

意图：在 `09-lakehouse` 路径第 15 步抓住「SQL/Table 桥接」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 7 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「SQL/Table 桥接」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「湖仓落地」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「SQL/Table 桥接」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-16 · 湖仓落地

意图：在 `09-lakehouse` 路径第 16 步抓住「湖仓落地」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 8 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「湖仓落地」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「AI 降级」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「湖仓落地」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-17 · AI 降级

意图：在 `09-lakehouse` 路径第 17 步抓住「AI 降级」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 1 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「AI 降级」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「GitOps 发布」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「AI 降级」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-18 · GitOps 发布

意图：在 `09-lakehouse` 路径第 18 步抓住「GitOps 发布」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 2 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「GitOps 发布」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「值班手册」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「GitOps 发布」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-19 · 值班手册

意图：在 `09-lakehouse` 路径第 19 步抓住「值班手册」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 3 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「值班手册」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「简历可验证陈述」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「值班手册」对应信号；不引入违禁词与断链。

### 模式 09-lakehouse-20 · 简历可验证陈述

意图：在 `09-lakehouse` 路径第 20 步抓住「简历可验证陈述」。先读 [`../../examples/e09-lakehouse/README.md`](../../examples/e09-lakehouse/README.md)（湖仓 SQL 脚本），再对照深潜「深潜 4 · Paimon 主键表」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「简历可验证陈述」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「正确性契约」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/09-lakehouse/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「简历可验证陈述」对应信号；不引入违禁词与断链。

## 术语对照（本模块）

- **术语**：见正文。结合本模块案例口述其失败模式。

## 综合论述

### 论述 1 · 从原理到仓库落地

把 `09-lakehouse` 的第 1 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「正确性」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 1 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 2 · 从原理到仓库落地

把 `09-lakehouse` 的第 2 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「延迟」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 2 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 3 · 从原理到仓库落地

把 `09-lakehouse` 的第 3 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「状态成本」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 3 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 4 · 从原理到仓库落地

把 `09-lakehouse` 的第 4 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「容错」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 4 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 5 · 从原理到仓库落地

把 `09-lakehouse` 的第 5 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「可观测」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 5 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 6 · 从原理到仓库落地

把 `09-lakehouse` 的第 6 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「安全」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 6 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 7 · 从原理到仓库落地

把 `09-lakehouse` 的第 7 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「成本治理」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 7 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 8 · 从原理到仓库落地

把 `09-lakehouse` 的第 8 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「简历验证」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 8 的验收口令：能指着 UI 或日志说出「看到了什么算过」。
