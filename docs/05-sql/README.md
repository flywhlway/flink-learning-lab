# 模块 05 · Flink SQL

> 覆盖章节:05-01 动态表与 changelog / 05-02 窗口 TVF / 05-03 Join 家族 / 05-04 去重与 Top-N / 05-05 优化开关 / 05-06 Catalog / 05-07 Materialized Table / 05-08 SQL AI 函数
> 配套实验:e05 × 10、playground P-01~P-20 · Level:L4

## 05-01 动态表与 changelog(一切的地基)

流 ⟷ 表的对偶:流是表的 changelog,表是流的物化。查询作用在动态表上,结果也是动态表,其变更以四种 RowKind 下发:+I 插入、-U/+U 更新对、-D 删除(e05-C1、e06-C5 亲手摸)。三类查询输出形态:**仅追加**(窗口聚合/interval join)、**回撤**(无窗口聚合/Top-N)、**upsert**(有主键下游时的优化形态,-U 可省)。工程铁律:写 SQL 前先回答"我的结果表是哪种形态、下游怎么消费"。

## 05-02 窗口 TVF 家族

TUMBLE/HOP/CUMULATE(+ SESSION)以表值函数形态出现,窗口列 window_start/window_end 变成普通列 —— 因此窗口后还能 join、Top-N(窗口 Top-N=仅追加榜单,e05-C3 注)。要点:HOP 状态×(size/slide);CUMULATE 专治"今日累计";TVF 查询必须 GROUP BY window_start, window_end 全组。

## 05-03 Join 家族与状态代价

| Join | 状态规模 | 清理依据 | 案例 |
|---|---|---|---|
| Regular | 双侧全历史 | 仅 TTL | e05-C6 |
| Interval | 双侧时间带内 | watermark | e05-C5 |
| Temporal(FOR SYSTEM_TIME) | 版本表全量+流侧无 | 版本演替/TTL | e05-C7 |
| Lookup | 无(点查外部) | — | e07-C2 |
| Window Join | 窗口内 | 窗口关闭 | TVF 衍生 |

选型见 e05/README 决策图;历史可复现要求 → Temporal;当下值 → Lookup。

## 05-04 去重与 Top-N

同一 ROW_NUMBER 骨架的两个特例:rn=1 去重(DESC=keep-last 回撤 / ASC=keep-first 可仅追加)、rn<=N Top-N(回撤;窗口内=仅追加)。planner 依赖**固定写法形态**识别,别自由发挥(e05 踩坑表)。COUNT(DISTINCT) 高基数三救济:开启 `table.optimizer.distinct-agg.split.enabled`(自动拆桶两级)、改近似(HLL UDAF)、下推 OLAP。

## 05-05 优化开关

`table.exec.mini-batch.*`(攒批摊薄状态访问)与 `table.optimizer.agg-phase-strategy=TWO_PHASE`(local-global 吸倾斜)是聚合作业默认三件套(e05-C8 看计划);`table.exec.state.ttl` 全局 + `STATE_TTL` hint 表级精调(e05-C10)。原则:**先窗口化/语义化,再 TTL,最后才是调参**。

## 05-06 Catalog 体系

三级命名空间与元数据接口(e06-C6);生产形态:Hive Metastore(存量)、Paimon Catalog(湖仓,e09)、JDBC Catalog(直读 PG 元数据)。平台价值:表即资产 —— 血缘、权限、审计都挂在 Catalog 上。

## 05-07 Materialized Table(2.x 流批一体的新答案)

`CREATE MATERIALIZED TABLE ... FRESHNESS = INTERVAL '1' MINUTE AS SELECT ...`:声明**新鲜度**而非调度方式,引擎自动选择连续流作业或周期批刷新,并管理依赖与回刷 —— 把"实时/离线两套 ETL"合并成一份 SQL。2.3 增强了分区级刷新与生态集成(见 docs/00 跟踪);实操将在 e09 湖仓底座就绪后补充演练。

## 05-08 SQL AI 函数(2.1+/2.2+)

`CREATE MODEL` 注册模型端点 → `ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(col))` 流上推理;`VECTOR_SEARCH` 流内向量检索(2.2)。这是 ai/ 第 3/4 章的主角,工程红线先记两条:推理必须有超时与降级;token 成本要计量(ai/18)。

## 知识总结 / 常见错误 / 企业实践 / 面试题 / 参考

**总结**:changelog 三形态 → 窗口 TVF → Join 五选一 → ROW_NUMBER 双特例 → 三开关 → TTL 分层 → Materialized Table 收束流批。
**常见错**:回撤流写 append 存储;Regular Join 裸奔无 TTL;把 CUMULATE 需求用 HOP 硬凑;UDF 里外呼。
**企业实践**:SQL 作业交付三件套(EXPLAIN 存档/TTL 论证/回撤去向)进模板与 CI(templates/job-sql)。
**面试**:interview 16~20;进阶:upsert-kafka 与普通 kafka sink 在 changelog 编码上的差异(-U 是否下发)?
**参考**:官方 Queries 全章、Performance Tuning、Materialized Table 文档;e05 源码。
