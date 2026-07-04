# best-practice/ · 生产军规(首批 12 条,P5 扩展为完整规范体系)

每条军规 = 规则 + 理由 + 反例事故。完整的架构/目录/命名/日志/CI-CD 规范在 P5 与 production/ 一起交付,先把这 12 条刻进流水线。

1. **有状态算子必须显式 `.uid()` 与 `.name()`**——savepoint 按 uid 匹配;曾有团队重构包名后全量状态作废。
2. **checkpoint 间隔 << Kafka 事务超时**,且 broker `transaction.max.timeout.ms` 放行——否则 EXACTLY_ONCE 反而造成数据丢失。
3. **watermark 必配 `withIdleness`**(除非能证明所有分区永远有流量)。
4. **禁止在算子里同步外呼**(HTTP/DB/LLM)——一律 Async I/O 或维表 Join;一个 200ms 的同步调用就能把吞吐钉死在 5×并行度 QPS。
5. **UV/去重默认用草图(HLL)或下推给 OLAP**,精确去重必须给出状态上界论证。
6. **脏数据走 side output 进死信 topic**,禁止 try-catch 吞掉——静默丢数是最难追查的事故形态。
7. **作业参数外置**,jar 不含环境信息;同一 jar 必须可在 dev/staging/prod 三态运行。
8. **升级必走 savepoint**,`stop --savepointPath` 而非 cancel;savepoint 至少保留最近 3 个。
9. **RocksDB/ForSt 大状态默认开增量 checkpoint**;>1GB 状态还没开 = 事故预约。
10. **每个作业上线前有反压基线**:知道它在多大 eps 下开始反压,值班才有判断力。
11. **SQL 作业必须显式设置 `table.exec.state.ttl`** 并论证 TTL 对正确性的影响——"默认永不过期"是状态爆炸的头号来源。
12. **AI 链路必须有降级路径**:LLM 超时/限流时事件走规则兜底或旁路存储,严禁让模型可用性决定数据链路可用性。(展开见 ai/ 第 III 部)
