# 模块 06 · Table API 与 DataStream 互转

> 配套实验:e06 × 8 · Level:L4

## 06-01 定位:三层 API 的分工

DataStream(命令式:时序/状态/外呼)、Table API(编程式声明:动态拼装、编译期检查)、SQL(文本声明:可评审可治理)。生产黄金比例:SQL 为主、DataStream 补个性化、Table API 做桥与平台胶水。

## 06-02 双向桥接(本模块核心)

- **入**:fromDataStream(+Schema:rowtime 列 + SOURCE_WATERMARK())/ fromChangelogStream(自带 RowKind 的 Row 流,自定义 CDC 入口,e06-C5)。
- **出**:仅追加 → toDataStream;有更新 → toChangelogStream(e06-C2)。
- **时间语义不断链**是桥接的验收标准(e06-C1/C8):Table 侧窗口能触发、回流后 watermark 仍推进。
- 混编无性能税:两界最终统一进同一优化器与拓扑(e06-C4)。

## 06-03 表达式 DSL 与函数

$()/lit()/call() 组合出与 SQL 等价的计划(e06-C3/C7);动态逻辑用 DSL 防注入,静态业务逻辑用 SQL 文本便于评审。UDF 两界共享注册。

## 06-04 Catalog 与元数据编程

listCatalogs/Databases/Tables、GenericInMemoryCatalog → 平台元数据服务原型(e06-C6);全限定名纪律防串环境。

## 知识总结 / 常见错误 / 企业实践 / 面试题 / 参考

**错**:桥接丢时间(症状:窗口不触发/wm=MIN);更新表走 toDataStream;平台代码拼 SQL 字符串(注入+漂移)。
**实践**:桥接点强制注释时间与 changelog 语义;平台动态逻辑一律 DSL。
**面试**:e06/README 第 6~8 节三问。
**参考**:官方 DataStream API Integration 全节;e06 源码。
