# interview/ · 面试题库(按 Level 分层)

> 使用方式:先自答再看提示;提示故意只给"考点骨架",完整推导回教材对应章节。首批 30 题,P5 扩至 150+ 并附完整参考答案。

## L1–L2(模型与时间)

1. 一个 Flink 作业从 `env.execute()` 到 Task 跑起来经历了哪几层图转换?各层分别在哪里生成?(01-02)
2. Slot 与并行度的关系?slot sharing 为什么能让"一个 slot 跑整条 pipeline"?(01-03)
3. 算子链的合并条件有哪些?什么时候你会手动 `disableChaining`?(01-03)
4. watermark 在多输入/多分区算子上如何合并?这个规则导致的最经典生产事故是什么?(02-02;答案骨架:取小;单分区静默拖死全局)
5. `forBoundedOutOfOrderness(5s)` 的 watermark 到底是怎么算的?乱序超过 5s 的数据去哪了?(02-02/02-04)
6. Event Time 与 Processing Time 各自适用的业务判据?"日活统计"用哪个,为什么?(02-01)
7. 会话窗口的合并机制?为什么它的状态清理比滚动窗口复杂?(02-03)
8. `withIdleness` 解决什么问题?它为什么救不了"全部分区都没数据"?(02-02)

## L3–L4(状态、容错与 SQL)

9. HashMap、RocksDB、ForSt 三种 state backend 的选型矩阵?(03-03)
10. Barrier 对齐与非对齐 checkpoint 的时序差异?非对齐的代价是什么?(04-01)
11. 增量 checkpoint 在 RocksDB 上是怎么实现的?为什么删除旧 checkpoint 不一定省空间?(04-01)
12. Checkpoint 与 Savepoint 的本质区别(不止"手动/自动")?(04-02)
13. 为什么每个有状态算子都要显式 `.uid()`?不设会发生什么?(04-02)
14. "checkpoint 成功但数据重复"——哪一环节的语义被误解了?端到端 exactly-once 的完整条件?(04-04)
15. Kafka Sink EXACTLY_ONCE 下,`transaction.timeout.ms` 与 checkpoint 间隔必须满足什么关系,为什么?(07-01)
16. 无窗口 `GROUP BY` 的 SQL 为什么产生回撤流?下游是 upsert-kafka 和普通 kafka 时分别怎么落地?(05-01)
17. Regular Join / Interval Join / Temporal Join 的状态规模分别由什么决定?(05-03)
18. `COUNT(DISTINCT)` 在流上为什么昂贵?三种降本手段?(05-04/e01 注释)
19. SQL 作业怎么设置状态 TTL?TTL 过期对正确性的影响如何评估?(05-05)
20. Materialized Table 解决什么问题?它与"定时批任务"的本质差异?(05-07)

## L5–L7(集成与平台)

21. Flink CDC YAML Pipeline 相比"DataStream 手写 CDC"的价值?Schema Evolution 如何处理?(08-01/08-02)
22. Paimon 与 Iceberg 在"流式更新"场景的根本差异?(09-05)
23. 反压的传导机制(credit-based)?定位反压根因的标准步骤?(01-05/13)
24. 数据倾斜的四类解法及适用条件?(13)
25. K8s 上 Application Mode 与 Session Mode 的选型判据?(14-01)
26. Operator 的三种升级模式对状态的保障差异?Blue/Green 发布适合什么场景?(14-01)

## L8+(AI 专项)

27. 什么样的 AI 场景应该进 Flink,什么不该?给出你的三条判据。(ai/ 第 0 节)
28. `ML_PREDICT` 把 LLM 调用放进流里,超时/限流/降级分别怎么设计?(ai/03)
29. Flink Agents 的"Action 级 exactly-once"是怎么和 checkpoint 协同的?工具副作用怎么办?(ai/09)
30. 流式 RAG 与常规 RAG 的差异点?"新鲜度"具体如何量化进架构?(ai/05)
