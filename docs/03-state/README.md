# 模块 03 · 状态

> 覆盖章节:03-01 Keyed State 五件套 / 03-02 Operator·Broadcast State / 03-03 后端选型(HashMap·RocksDB·ForSt)/ 03-04 RocksDB 调优 / 03-05 TTL 与膨胀治理 / 03-06 Side Output 模式集 / 03-07 序列化与 schema 演进
> 配套实验:e03 全部 10 案例 · Level:L3

## 03-01 Keyed State 五件套

| 类型 | 语义 | RocksDB 物理形态 | 案例 |
|---|---|---|---|
| ValueState\<T\> | 每 key 单值 | 1 个 KV | e03-C1 |
| ListState\<T\> | 每 key 追加序列 | 1 个 KV,值为拼接字节;add 追加、get 全量反序列化 | e03-C2 |
| MapState\<K,V\> | 每 key 一个字典 | **每 entry 独立 KV**,点读点写 | e03-C3 |
| ReducingState\<T\> | 每 key 滚动 reduce(IN=OUT) | 1 个 KV(聚合值) | —(C4 的特例) |
| AggregatingState\<IN,OUT\> | 每 key 滚动聚合(可异型) | 1 个 KV(累加器) | e03-C4 |

选择口诀:**单值 Value、短序列 List、字典/长序列 Map、能增量聚合就 Agg**。两条红线:① `ValueState<HashMap>` 是反模式(整体序列化,C3 对照);② ListState 超过~百级元素改 `MapState<Long,T>` 环形缓冲(index 存 ValueState)。

Key 的隐形機制:状态按 **key group** 组织(`0..maxParallelism-1` 个桶),`keyGroup = murmurHash(key) % maxParallelism`,subtask 按区间认领 key group —— 这就是扩缩容时 keyed state 能重分布的原因,也是 `maxParallelism`(默认 128,进 savepoint 后**不可更改**)必须在第一天就规划好的原因。

## 03-02 Operator State 与 Broadcast State

- **Operator State**(e03-C5):与 key 无关、按 subtask 存;典型宿主是 Source(Kafka offset)与攒批 Sink。恢复重分发:`getListState` = even-split(切分给各 subtask),`getUnionListState` = 每个 subtask 拿全量副本(仅适合"人人都要完整台账"的场景,且随并行度膨胀,慎用)。
- **Broadcast State**(e03-C7):Operator State 的特例——每个并行实例持有**同一份**Map;写入只能发生在 `processBroadcastElement`,且必须确定性一致(禁止依赖随机数/本地时钟/到达顺序以外的东西);`processElement` 侧只读。四大金刚场景:动态规则、小维表广播、AB 开关、黑白名单。规模红线:Broadcast State 全内存、不落 RocksDB,MB 级为宜,百 MB 级改 Lookup Join。

## 03-03 State Backend 选型:HashMap vs RocksDB vs ForSt

```mermaid
flowchart TD
    Q1{状态规模?} -->|GB 级以下且追求极致延迟| HM[HashMapStateBackend\n堆内对象,读写零序列化]
    Q1 -->|GB~TB 级| Q2{是否需要快速扩缩容\n/ 秒级恢复 / 云原生弹性?}
    Q2 -->|否,单机盘够| RD[RocksDB\n本地 LSM,增量 checkpoint]
    Q2 -->|是| FS[ForSt(2.x 新)\n存算分离:状态主体在对象存储\n+ 本地缓存 + 异步状态 API]
```

三者一句话:**HashMap 快但受限于堆;RocksDB 大但绑本地盘;ForSt 把状态搬到 S3/OSS,checkpoint 与 rescale 从"拷数据"变"改指针"**。ForSt 是 Flink 2.0 存算分离叙事的核心(配合异步状态访问 API 掩盖远端延迟),重状态 + 弹性优先的新作业值得首选评估;成熟稳态大作业 RocksDB 仍是默认答案。切换方式统一走配置(`state.backend.type: hashmap|rocksdb|forst`),e03-C10 演示了配置化切换的姿势。

## 03-04 RocksDB 深度调优(五个真正常用的旋钮)

1. **managed memory 份额**:`taskmanager.memory.managed.fraction` 0.5~0.7,块缓存+memtable 都从这里出(01-04 联动);
2. **增量 checkpoint**:`execution.checkpointing.incremental: true`,GB 级以上状态必开(C10 观察);
3. **本地盘**:`state.backend.rocksdb.localdir` 指向 NVMe;容器里注意别落到 overlayfs;
4. **分区索引/过滤器**:`state.backend.rocksdb.memory.partitioned-index-filters: true`,大状态点查内存友好;
5. **写放大观察**:compaction 打满 CPU 时,优先查"是不是把 List 当队列疯狂重写"(数据结构问题),再谈调 `write-buffer` 系列参数。

原则:**先改数据结构与访问模式(03-01 的红线),再动 RocksDB 参数**——90% 的"RocksDB 慢"是用法问题。

## 03-05 State TTL 与膨胀治理

TTL 三元组(e03-C6):
- **UpdateType**:OnCreateAndWrite(默认)/ OnReadAndWrite——决定"活跃"的定义;
- **StateVisibility**:NeverReturnExpired(默认,安全)/ ReturnExpiredIfNotCleanedUp(容忍读到僵尸,换性能);
- **清理策略**:惰性(读时判死)+ 增量清理(每次访问顺带扫 N 条)+ RocksDB compaction filter(压实时物理删)。

治理观:TTL 是**保险丝**不是设计。膨胀治理的顺序:① 业务上界论证(军规 5/11)→ ② 聚合口径瘦身(精确去重改草图)→ ③ TTL 兜底 → ④ State Processor API 离线洗状态(04-03)。SQL 作业的 `table.exec.state.ttl` 同理,且 Regular Join 双侧状态默认**永不过期**,不设 TTL 等于事故预约。

## 03-06 Side Output 模式集

四个标准用法(全部一次遍历、零额外扫描):
1. **死信通道**(e03-C8):脏数据 → 死信 topic + 对账任务,军规 6 的实现;
2. **迟到旁路**(e02-C3):`sideOutputLateData`;
3. **分级路由**:主流/告警/审计各走各的 Sink;
4. **采样/调试流**:千分之一采样旁路到观测存储,生产排障利器。

技术细节:OutputTag 匿名子类保泛型;side output 在 ProcessFunction/窗口/CoProcess 均可用;各路可以是**不同类型**(主流 Event、旁路 String)。

## 03-07 状态序列化与 schema 演进

- 状态的序列化器在**首次写入时固化**并存入快照;恢复时新旧序列化器做兼容性判定。
- 演进能力矩阵:POJO 支持加/删字段(不支持改类型/改类名);Avro 支持 schema evolution 全家桶;**Kryo 完全不支持**——状态里出现 Kryo 类型 = 未来某次升级必然翻车,这是 01-06 红线在状态维度的加强版。
- 类名即契约:状态 POJO 移包/改名等于换类型;真要重构,走 State Processor API 迁移(04-03)。
- 自定义 TypeSerializer 时必须同时实现 `TypeSerializerSnapshot`(版本化+兼容性判定),否则升级链路断裂。

## 知识总结与重点

状态设计四问(设计评审必答):**存什么(类型五件套)、多大(上界论证)、多久(TTL/清理)、怎么演进(POJO/Avro 契约)**。重点:key group 与 maxParallelism、MapState vs ValueState\<Map\>、Broadcast 确定性纪律、三后端决策树、TTL 三元组、Kryo 状态红线。

## 常见错误

descriptor 每条消息重建;在 `processElement` 里写 Broadcast State(API 就不给,别绕);Union redistribution 滥用导致恢复 OOM;maxParallelism 上线后想改(改不了,只能 State Processor API 重写);把 TTL 当精确定时器(该用 Timer,e03-C9)。

## 企业实践

平台侧沉淀《状态档案》模板:每个作业登记状态清单(名称/类型/预估规模/TTL/演进策略),纳入上线评审与容量巡检;RocksDB 作业统一接入 `state.backend.rocksdb.metrics.*` 关键指标(block cache 命中率、compaction pending)进 Grafana(monitoring/ 模块)。

## 面试题

interview/README 9、TTL 三语义、Broadcast 为什么不支持 keyed 访问、增量 checkpoint 空间回收(题 11)、key group 机制与 maxParallelism 不可变性;进阶:*ForSt 与 RocksDB 在 checkpoint 与 rescale 两个动作上的本质差异?*

## 参考资料

官方 DataStream→Working with State / Broadcast State / State TTL;State Backends 文档;FLIP-423/428(Disaggregated State & ForSt);RocksDB Wiki(LSM/Compaction);e03 十案例源码。
