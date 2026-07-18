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
