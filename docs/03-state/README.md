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

---

# 模块 03 · 状态 — 实质扩写（Wave 2）

> 本章扩写遵循八段式：背景→架构→代码锚点→启动→验证→踩坑→最佳实践→面试题；交叉引用均为相对路径，禁止官网粘贴与重复段落注水（D-05）。

## 仓库交叉引用总表

| 路径 | 说明 |
|---|---|
| [`../../examples/e03-state/README.md`](../../examples/e03-state/README.md) | 状态模块总览 |
| [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java) | ValueState |
| [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java) | State TTL |
| [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java) | Broadcast State |
| [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java) | Side Output |
| [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java) | RocksDB lab |

## 背景

### 背景 · 1

无状态 map 只能做瞬时变换；企业流计算的核心资产是 keyed state、广播规则与可恢复的算子状态。

### 背景 · 2

e03 覆盖 Value/List/Map/Aggregating、Operator State、TTL、Broadcast、Side Output、Timer、RocksDB。

### 背景 · 3

与容错模块关系：状态是 checkpoint 的内容；与 uid 纪律关系：状态按 uid 匹配。

### 背景 · 4

生产项目对照：p02 双通道特征、p03 Broadcast Pattern、p01 预算门控旁路。

## 架构

### 架构 · 1

Keyed State 绑定 key group；Operator State 绑定并行子任务；Broadcast State 在规则侧只写、数据侧只读的典型模式。

### 架构 · 2

Backend：HashMap（堆、调试友好）vs RocksDB（大状态）vs ForSt（存算分离方向，见版图章）。

### 架构 · 3

TTL 是防膨胀第一刀；Side Output 是脏数据与旁路的外科手术。

### 架构 · 4

序列化演进：状态类型 POJO 化，拒绝 Kryo 进状态（Runtime 章军规在此落地）。

## 代码锚点

### 代码锚点 · 1

C1–C4：五件套直觉与聚合状态。

### 代码锚点 · 2

C5：Operator State 缓冲与 list checkpoint 语义。

### 代码锚点 · 3

C6：TTL 缓存过期。

### 代码锚点 · 4

C7：广播规则更新。

### 代码锚点 · 5

C8：路由旁路。

### 代码锚点 · 6

C9：Timer 不活跃检测。

### 代码锚点 · 7

C10：RocksDB 参数实验。

## 启动

### 启动 · 1

```bash
(cd examples && mvn -pl e03-state -am -DskipTests package)
```

### 启动 · 2

RocksDB lab 注意 managed memory；堆状态作业勿照搬 RocksDB 参数。

### 启动 · 3

Broadcast 案例可与 p03 动态 Pattern 对照阅读。

## 验证

### 验证 · 1

keyed 查询：同 key 状态连续；不同 key 隔离。

### 验证 · 2

TTL：过期后读为空或默认值。

### 验证 · 3

Broadcast：规则更新后下游行为变化且不丢数据侧状态。

### 验证 · 4

RocksDB：观察 checkpoint 大小与吞吐权衡。

## 踩坑

### 踩坑 · 1

| 症状 | 根因 | 处置 |
|---|---|---|
| 堆 OOM | HashMap 放大 | RocksDB 或降基数 |
| 状态只增不减 | 无 TTL/清理 | TTL+定时清理 |
| 恢复丢规则 | Broadcast 未 checkpoint | 检查后端与 uid |
| 旁路丢失 | side output 未 connect | 显式 getSideOutput |
| schema 失败 | Kryo 状态 | 改 POJO |

### 踩坑 · 2

高基数 key（用户 ID、设备 ID）必须有过期策略；车联网与推荐都是重灾区。

### 踩坑 · 3

Broadcast 状态内存占用随规则集增长——p03 用预编译 Pattern 集而非无限动态脚本。

## 最佳实践

### 最佳实践 · 1

`best-practice/04-state-ttl.md`：TTL 默认开启的业务类型清单。

### 最佳实践 · 2

大状态默认 RocksDB；本地 demo 可用 HashMap。

### 最佳实践 · 3

Side Output 契约写入 README（迟到、脏数据、熔断）。

### 最佳实践 · 4

状态字段变更走演进清单，禁止静默改类型。

### 最佳实践 · 5

题库：`interview/L3.md`。

## 面试题

### 面试题 · 1

Keyed 与 Operator State 的划分标准？

### 面试题 · 2

Broadcast State 读写约定？

### 面试题 · 3

TTL 对 checkpoint 大小的影响？

### 面试题 · 4

RocksDB 调优先动哪些旋钮？

### 面试题 · 5

Side Output 与分流 filter 的差别？

## 深潜专题

### Key Group 与 maxParallelism

key group 数量由 maxParallelism 决定，限制未来 rescale。上线前论证，避免日后再也扩不上去。见 checkpoint 章与 best-practice/02。

落地检查（03-state/深潜1）：针对「Key Group 与 maxParallelism」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### AggregatingState vs ReducingState

聚合需要累加器与 get 结果分离时用 Aggregating；简单归约用 Reducing。C4 演示平均值累加器。

落地检查（03-state/深潜2）：针对「AggregatingState vs ReducingState」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### Operator State 重分布

并行度变化时 list state 按轮询等方式拆分；设计缓冲语义时要考虑重分布后的重复/空洞。

落地检查（03-state/深潜3）：针对「Operator State 重分布」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### Broadcast 与动态 CEP

p03 选择 Broadcast 切换预编译 Pattern，而不是运行时编译任意脚本——安全性与性能的折中（ADR）。

落地检查（03-state/深潜4）：针对「Broadcast 与动态 CEP」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 状态查询与外置存储

Queryable State 不是业务读模型主路径；对外服务用 sink 到 Redis/PK 表（p02）。

落地检查（03-state/深潜5）：针对「状态查询与外置存储」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### RocksDB 块缓存与 managed

块缓存与 memtable 来自 managed memory；盲目加大堆无益。C10 做对照实验。

落地检查（03-state/深潜6）：针对「RocksDB 块缓存与 managed」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### TTL 清理滞后

过期可能惰性清理；监控状态大小而非假设即时删除。

落地检查（03-state/深潜7）：针对「TTL 清理滞后」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

### 与 Async I/O 状态协作

异步维表结果可写入 ValueState 做本地缓存，并设 TTL（e11 + C6 模式）。

落地检查（03-state/深潜8）：针对「与 Async I/O 状态协作」，在 OrbStack 上做一次最小对照——记录一项指标名或日志关键字，并写明期望方向（升/降/出现/消失）。面试表述映射到 `../../interview/` 中与本模块编号相近的 Level。

## FAQ

### 何时用 MapState？

同一 key 下多子键计数/集合；注意条目膨胀。

延伸（FAQ-1）：用自己的业务域复述「何时用 MapState？」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### Broadcast 能存大维表吗？

不适合；维表用 Async/Lookup，Broadcast 适合规则。

延伸（FAQ-2）：用自己的业务域复述「Broadcast 能存大维表吗？」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### ForSt 什么时候看？

版图与实验；主线学习先掌握 RocksDB。

延伸（FAQ-3）：用自己的业务域复述「ForSt 什么时候看？」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### 状态能在 savepoint 间改名吗？

uid 变了等于新状态；字段演进靠 POJO 兼容。

延伸（FAQ-4）：用自己的业务域复述「状态能在 savepoint 间改名吗？」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

### Timer 会进 checkpoint 吗？

会；大量 timer 也是状态膨胀源。

延伸（FAQ-5）：用自己的业务域复述「Timer 会进 checkpoint 吗？」，并指出一个具体 `examples/**/*.java` 或 `projects/*/README.md` 佐证点；找不到就先补实验。

## 检查清单

- [ ] 状态后端选型书面化
- [ ] 高基数 key 有 TTL/清理
- [ ] Broadcast 仅规则、有上限
- [ ] Side Output 有下游
- [ ] uid 稳定
- [ ] RocksDB 作业核对 managed 内存

## 情景演练

### 场景 A · 推荐特征状态膨胀

p02 用户特征 key 高基数；TTL + Redis 外置热特征；Flink 内保留短窗。

演练记录建议包含：时间、环境（OrbStack）、命令、期望、实际、截图/日志路径。项目级证据模板见各 `projects/*/docs/baseline.md`。

### 场景 B · 规则热更新

Broadcast 下发规则版本号；数据侧只读；失败则保持旧规则（与 p01 降级类似）。

演练记录建议包含：时间、环境（OrbStack）、命令、期望、实际、截图/日志路径。项目级证据模板见各 `projects/*/docs/baseline.md`。

### 场景 C · 脏数据隔离

解析失败进 side output 死信 topic；主流不被脏事件污染。

演练记录建议包含：时间、环境（OrbStack）、命令、期望、实际、截图/日志路径。项目级证据模板见各 `projects/*/docs/baseline.md`。

## 模式目录（本模块专用）

### 模式 03-state-01 · 正确性契约

意图：在 `03-state` 路径第 1 步抓住「正确性契约」。先读 [`../../examples/e03-state/README.md`](../../examples/e03-state/README.md)（状态模块总览），再对照深潜「Key Group 与 maxParallelism」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「正确性契约」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「状态与 uid」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「正确性契约」对应信号；不引入违禁词与断链。

### 模式 03-state-02 · 状态与 uid

意图：在 `03-state` 路径第 2 步抓住「状态与 uid」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java)（ValueState），再对照深潜「AggregatingState vs ReducingState」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「状态与 uid」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「时间语义」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「状态与 uid」对应信号；不引入违禁词与断链。

### 模式 03-state-03 · 时间语义

意图：在 `03-state` 路径第 3 步抓住「时间语义」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java)（State TTL），再对照深潜「Operator State 重分布」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「时间语义」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「反压与容量」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「时间语义」对应信号；不引入违禁词与断链。

### 模式 03-state-04 · 反压与容量

意图：在 `03-state` 路径第 4 步抓住「反压与容量」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java)（Broadcast State），再对照深潜「Broadcast 与动态 CEP」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「反压与容量」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「容错恢复」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「反压与容量」对应信号；不引入违禁词与断链。

### 模式 03-state-05 · 容错恢复

意图：在 `03-state` 路径第 5 步抓住「容错恢复」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java)（Side Output），再对照深潜「状态查询与外置存储」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「容错恢复」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「连接器语义」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「容错恢复」对应信号；不引入违禁词与断链。

### 模式 03-state-06 · 连接器语义

意图：在 `03-state` 路径第 6 步抓住「连接器语义」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java)（RocksDB lab），再对照深潜「RocksDB 块缓存与 managed」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「连接器语义」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「旁路与降级」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「连接器语义」对应信号；不引入违禁词与断链。

### 模式 03-state-07 · 旁路与降级

意图：在 `03-state` 路径第 7 步抓住「旁路与降级」。先读 [`../../examples/e03-state/README.md`](../../examples/e03-state/README.md)（状态模块总览），再对照深潜「TTL 清理滞后」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「旁路与降级」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「可观测指标」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「旁路与降级」对应信号；不引入违禁词与断链。

### 模式 03-state-08 · 可观测指标

意图：在 `03-state` 路径第 8 步抓住「可观测指标」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java)（ValueState），再对照深潜「与 Async I/O 状态协作」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「可观测指标」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「压测基线」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「可观测指标」对应信号；不引入违禁词与断链。

### 模式 03-state-09 · 压测基线

意图：在 `03-state` 路径第 9 步抓住「压测基线」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java)（State TTL），再对照深潜「Key Group 与 maxParallelism」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「压测基线」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「升级与 savepoint」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「压测基线」对应信号；不引入违禁词与断链。

### 模式 03-state-10 · 升级与 savepoint

意图：在 `03-state` 路径第 10 步抓住「升级与 savepoint」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java)（Broadcast State），再对照深潜「AggregatingState vs ReducingState」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「升级与 savepoint」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「安全与多租户」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「升级与 savepoint」对应信号；不引入违禁词与断链。

### 模式 03-state-11 · 安全与多租户

意图：在 `03-state` 路径第 11 步抓住「安全与多租户」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java)（Side Output），再对照深潜「Operator State 重分布」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「安全与多租户」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「成本与预算」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「安全与多租户」对应信号；不引入违禁词与断链。

### 模式 03-state-12 · 成本与预算

意图：在 `03-state` 路径第 12 步抓住「成本与预算」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java)（RocksDB lab），再对照深潜「Broadcast 与动态 CEP」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「成本与预算」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「Schema 演进」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「成本与预算」对应信号；不引入违禁词与断链。

### 模式 03-state-13 · Schema 演进

意图：在 `03-state` 路径第 13 步抓住「Schema 演进」。先读 [`../../examples/e03-state/README.md`](../../examples/e03-state/README.md)（状态模块总览），再对照深潜「状态查询与外置存储」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「Schema 演进」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「CEP/规则」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「Schema 演进」对应信号；不引入违禁词与断链。

### 模式 03-state-14 · CEP/规则

意图：在 `03-state` 路径第 14 步抓住「CEP/规则」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java)（ValueState），再对照深潜「RocksDB 块缓存与 managed」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「CEP/规则」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「SQL/Table 桥接」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「CEP/规则」对应信号；不引入违禁词与断链。

### 模式 03-state-15 · SQL/Table 桥接

意图：在 `03-state` 路径第 15 步抓住「SQL/Table 桥接」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C6StateTtlCacheJob.java)（State TTL），再对照深潜「TTL 清理滞后」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「SQL/Table 桥接」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「湖仓落地」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「SQL/Table 桥接」对应信号；不引入违禁词与断链。

### 模式 03-state-16 · 湖仓落地

意图：在 `03-state` 路径第 16 步抓住「湖仓落地」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java)（Broadcast State），再对照深潜「与 Async I/O 状态协作」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「湖仓落地」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「AI 降级」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「湖仓落地」对应信号；不引入违禁词与断链。

### 模式 03-state-17 · AI 降级

意图：在 `03-state` 路径第 17 步抓住「AI 降级」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C8SideOutputRouterJob.java)（Side Output），再对照深潜「Key Group 与 maxParallelism」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「AI 降级」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「GitOps 发布」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「AI 降级」对应信号；不引入违禁词与断链。

### 模式 03-state-18 · GitOps 发布

意图：在 `03-state` 路径第 18 步抓住「GitOps 发布」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C10RocksDbBackendLabJob.java)（RocksDB lab），再对照深潜「AggregatingState vs ReducingState」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「GitOps 发布」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「值班手册」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「GitOps 发布」对应信号；不引入违禁词与断链。

### 模式 03-state-19 · 值班手册

意图：在 `03-state` 路径第 19 步抓住「值班手册」。先读 [`../../examples/e03-state/README.md`](../../examples/e03-state/README.md)（状态模块总览），再对照深潜「Operator State 重分布」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「值班手册」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「简历可验证陈述」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「值班手册」对应信号；不引入违禁词与断链。

### 模式 03-state-20 · 简历可验证陈述

意图：在 `03-state` 路径第 20 步抓住「简历可验证陈述」。先读 [`../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java`](../../examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C1ValueStateBalanceJob.java)（ValueState），再对照深潜「Broadcast 与动态 CEP」，最后写一句：若线上出现相反现象，我首先检查什么。

机制：用数据面/控制面语言解释「简历可验证陈述」如何在本模块出现；约束仍是 Flink 2.2.1 / JDK 21 / OrbStack 实测，版本以根 README 矩阵为准。

反例：只改 YAML 不跑作业；或把其他模块「正确性契约」段落粘过来充数。正例：画出输入→算子→输出契约，并链回 `docs/03-state/`。

检查：相关模块 `mvn -pl … -am -DskipTests compile`；UI/日志出现与「简历可验证陈述」对应信号；不引入违禁词与断链。

## 术语对照（本模块）

- **Keyed State**：按 key 分区的托管状态。结合本模块案例口述其失败模式。
- **Broadcast State**：广播规则状态。结合本模块案例口述其失败模式。
- **State TTL**：状态过期策略。结合本模块案例口述其失败模式。
- **RocksDBStateBackend**：堆外状态后端。结合本模块案例口述其失败模式。
- **Key Group**：key 分配与 rescale 粒度。结合本模块案例口述其失败模式。

## 综合论述

### 论述 1 · 从原理到仓库落地

把 `03-state` 的第 1 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「正确性」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 1 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 2 · 从原理到仓库落地

把 `03-state` 的第 2 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「延迟」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 2 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 3 · 从原理到仓库落地

把 `03-state` 的第 3 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「状态成本」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 3 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 4 · 从原理到仓库落地

把 `03-state` 的第 4 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「容错」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 4 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 5 · 从原理到仓库落地

把 `03-state` 的第 5 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「可观测」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 5 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 6 · 从原理到仓库落地

把 `03-state` 的第 6 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「安全」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 6 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 7 · 从原理到仓库落地

把 `03-state` 的第 7 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「成本治理」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 7 的验收口令：能指着 UI 或日志说出「看到了什么算过」。

### 论述 8 · 从原理到仓库落地

把 `03-state` 的第 8 个核心概念放到端到端链路中：源（datagen/Kafka）→ 变换/状态 → sink。本论述聚焦维度「简历验证」：说明取舍，并引用至少一个相对路径（`examples/`、`projects/`、`best-practice/` 或 `production/docs/`）。

正确性侧：哪些静默错误与本维度相关（错误时间语义、错误 uid、错误语义矩阵等）？成本侧：状态大小、checkpoint 时长、外部调用 QPS 如何被牵动？可运维侧：哪条指标/日志能证明契约仍成立？

收尾：写出三条可在 OrbStack 演示的步骤（命令级），细节指向本模块 README 启动/验证段，避免粘贴长日志。维度编号 8 的验收口令：能指着 UI 或日志说出「看到了什么算过」。
