# Phase 2: p03 模式库与 Broadcast - Research

**Researched:** 2026-07-18
**Domain:** Apache Flink CEP 模式库（五元组）+ Broadcast State 动态选择预编译模式集
**Confidence:** HIGH

## Summary

Phase 2 在已交付的 `projects/p03-vehicle-monitoring` 单模式告警链路上，落地 **恰好 3 条预编译 CEP 模式**（`HARSH_THEN_FAULT` / `TRIPLE_HARSH` / `DTC_PAIR`），每条登记五元组文档，并以 **静态多 CEP 作业图 + Broadcast 出口门控** 实现运行期切换激活集——不引入商业动态 CEP、不运行时编译 Pattern。控制面走 Kafka topic `vehicle.pattern.control`，消息为确定性 JSON；ClickHouse 增加 `pattern_id` 并继续作为 verify 唯一权威出口。

仓库已具备全部机制样板：`HarshThenFaultPattern` + Handler、e10-C1（`times(3).consecutive()`）、e03-C7 / e12-17（Broadcast 纪律）、Phase 1 造数/verify/WM 尾心跳纪律。本 Phase 的工程焦点是 **注册表 + 多分支接线 + 门控确定性 + within 自检 + 可切换 e2e**，而非新中间件或新依赖。

**Primary recommendation:** 在 `VehicleAlertJob` 中并行挂载 3 条 `CEP.pattern(...).within(...)`，Handler 输出带 `patternId`；union 后经 `BroadcastProcessFunction`（或 `keyBy(vin)` + `KeyedBroadcastProcessFunction`）按 Broadcast State 中的 `activePatterns` 过滤；`p03-init` 幂等创建 `vehicle.pattern.control`；DDL/`ClickHouseAlertSink` 增加 `pattern_id`；`PATTERN-LIBRARY.md` + 注册表单测强制五元组/`within`；造数/verify 按 `pattern_id` 演示切换。

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** 模式库固定交付 **恰好 3 条**预编译模式（满足 ≥3，避免本 Phase 膨胀），全部复用现有信号白名单 `HARSH_ACCEL | DTC | HEARTBEAT`，不新增 signalType（Parse 层零扩展）。
- **D-02:** 三条模式职责分工（教学覆盖面优先）：
  1. `HARSH_THEN_FAULT` — 保留 Phase 1：`followedBy` + `within(30s)`（基线）
  2. `TRIPLE_HARSH` — `times(3).consecutive()` 急加速突发 + `within(20s)`（量词/紧邻）
  3. `DTC_PAIR` — `DTC followedBy DTC` + `within(15s)` + 显式 `AfterMatchSkipStrategy.skipPastLastEvent()`（重复故障 + skip 语义）
- **D-03:** 每条模式必须在独立文档登记五元组：**业务含义 / within / 连接语义 / skip 策略 / 状态上界**。落点：`projects/p03-vehicle-monitoring/docs/PATTERN-LIBRARY.md`（八段式 README 交叉引用该页，不把五元组只写在散文里）。
- **D-04:** 控制面走 **Kafka topic** `vehicle.pattern.control`（由现有 `p03-init` 幂等创建），消息为确定性 JSON：`{"activePatterns":["HARSH_THEN_FAULT"],"version":N}`。禁止依赖本地文件热读或作业参数重启换模式作为验收主路径。
- **D-05:** Broadcast 写入纪律对齐 e03-C7：仅在 `processBroadcastElement` 写 Broadcast State；内容完全来自广播消息本身（禁止随机数/本地时钟）；`processElement` 侧只读。
- **D-06:** 默认激活集为 `["HARSH_THEN_FAULT"]`，保证 Phase 1 造数/verify 路径在未发控制消息时仍可绿。
- **D-07:** **静态作业图 + 出口门控**（推荐默认）：图中并行挂载 3 条 `CEP.pattern(...)`（各自强制 `within`），Handler 输出带 `patternId`；union 后经 **Broadcast 门控算子**按 `activePatterns` 过滤再双写 Kafka/ClickHouse。禁止引入商业动态 CEP / 运行时编译 Pattern。
- **D-08:** `AlertEvent` 增加 `patternId` 字段；ClickHouse `vehicle_alerts` 增加对应列（幂等 DDL / 兼容迁移由 planner 落地），verify 可按 `pattern_id` 断言切换效果。
- **D-09:** TIMEOUT Side Output 语义保留：超时告警同样带 `patternId`，并受同一激活集门控（未激活模式的 TIMEOUT 不落库）。
- **D-10:** 新增（或扩展）可执行验收：发布控制消息切换激活集 → 造数命中模式 A/B → **ClickHouse 为唯一权威出口**断言 `pattern_id` 与匹配行为变化（延续 Phase 1：Kafka 仅诊断）。
- **D-11:** 项目自检：**无 `within` 不得合入**——以模式注册表 + 单测（或等价静态检查）强制每条工厂方法含 `within`；文档评审清单勾选五元组缺项即失败。
- **D-12:** 造数脚本为每条模式提供可判定 `--scenario`（至少 `match-harsh-fault` / `match-triple-harsh` / `match-dtc-pair` + 控制消息辅助命令），尾心跳推进 watermark 纪律延续 Phase 1。

### Claude's Discretion
- 门控算子具体类名、Broadcast State descriptor key、控制消息 schema 字段命名细节可由 researcher/planner 按 e03-C7 / e12-17 类比选定，只要满足 D-04–D-09。
- `TRIPLE_HARSH` / `DTC_PAIR` 的阈值与 within 秒数可在实现时微调，但必须写进五元组且可被造数稳定触发。

### Deferred Ideas (OUT OF SCOPE)
- Grafana 窗口聚合大盘与异常检测面板 → Phase 3（VEH-05）
- 压测脚本 / watermark 停滞故障演练 / baseline 数字 → Phase 3（VEH-06）
- 完整 ADR + 简历陈述页终稿 → Phase 3（VEH-07）
- 商业动态 CEP / 运行时编译 Pattern → 明确拒绝（STACK / FUT）
- 新增 signalType（如 SPEED）以支撑更多模式 → 若需要，另开 backlog；本 Phase 刻意不扩展
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| VEH-03 | 模式库至少 3 条模式，每条登记 within/连接语义/skip/状态上界五元组 | D-01–D-03；`PATTERN-LIBRARY.md` + `PatternRegistry`；e10 README 五元组模板；C1/C5/skip API |
| VEH-04 | 作业支持通过 Broadcast 动态选择预编译模式集 | D-04–D-10；静态三 CEP + 出口门控；e03-C7 / Flink Broadcast State 官方纪律；control topic + version 单调更新 |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| 模式定义 / within / skip / 注册表 | API / Backend（Flink Job 编译期） | CDN / Static（文档 PATTERN-LIBRARY） | Pattern 编译期固定；文档供评审 |
| CEP 匹配（3 分支并行） | API / Backend（Flink CEP NFA） | — | 每 key(vin) 独立 NFA；状态在 TM |
| 激活集控制面 | Database / Storage（Kafka control topic） | API / Backend（Broadcast State） | 运维发 JSON；作业内复制到各并行实例 |
| 出口门控（按 patternId 过滤） | API / Backend（BroadcastProcessFunction） | — | 只读 Broadcast State；不改 CEP 图 |
| 告警落库 / 诊断通道 | Database / Storage（ClickHouse + Kafka alerts） | — | CH 权威；Kafka 诊断 |
| 造数 / 发控制消息 / verify | API / Backend（宿主机脚本） | — | 可判定场景 + CH `pattern_id` 断言 |
| topic / DDL 初始化 | CDN / Static（Compose `p03-init`） | Database / Storage | profile 隔离；不进 default `make up` |

## Project Constraints (from .cursor/rules/)

未发现 `.cursor/rules/` 目录。本阶段以仓库根 `CLAUDE.md` / `PHASES.md` / 根 README 工程约定为准：

- Flink 2.2.1 / JDK 21 SSOT；新增组件先登记版本矩阵
- OrbStack arm64 实测；不可验证不合入
- 禁止 TODO / 省略 / 略 / 自行实现 / 请参考官网
- 文档八段式；`docs/README.md` 先登记编号
- Phase 结束跑 `scripts/qa_check.sh`；更新 CHANGELOG 未发布区 + PHASES.md

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Apache Flink | 2.2.1 | 作业运行时 | SSOT；镜像 `flink:2.2.1-java21` [VERIFIED: projects/p03 pom + examples/pom] |
| JDK | 21 | 编译与运行 | 本机 `openjdk 21.0.2` [VERIFIED: env] |
| flink-cep | 2.2.1 | Pattern / NFA / AfterMatchSkipStrategy | 随 Flink；`javap` 确认 `begin(name, skip)` / `within(Duration)` / `skipPastLastEvent()` [VERIFIED: local jar + CITED: Flink 2.2 CEP docs] |
| flink-connector-kafka | 5.0.0-2.2 | events + control + alerts | 已在 p03 pom [VERIFIED: p03 pom] |
| flink-streaming-java | 2.2.1 | BroadcastProcessFunction / KeyedBroadcastProcessFunction | e03-C7 样板 [CITED: Flink 2.2 Broadcast State docs] |
| Kafka | apache/kafka:3.9.1 KRaft | vehicle.events / .alerts / .pattern.control | 基座已有；control topic 由 p03-init 追加 [VERIFIED: docker] |
| ClickHouse | 24.8 | vehicle_alerts + pattern_id | Phase 1 表；本 Phase 加列 [VERIFIED: docker/.env + sql] |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| ClickHouse HTTP SinkV2 | 手写（现有 ClickHouseAlertSink） | INSERT 含 pattern_id | 扩展现有 sink，勿引入未登记连接器 |
| Jackson / SimpleStringSchema | 随 Flink / 现有 | 控制消息与 AlertEvent JSON | 对齐 ParseVehicleJson / AlertEventJsonSchema |
| JUnit Jupiter | 5.10.2 | within / 注册表 / 门控单测 | 已有 surefire 3.2.5 [VERIFIED: p03 pom] |
| confluent-kafka (Python) | ≥2.5（本机 2.15.0） | 造数 + 发控制消息 | uv script；slopcheck [OK] [VERIFIED: PyPI + slopcheck] |
| uv | 0.10.10 | 运行 Python 脚本 | 与 Phase 1 一致 [VERIFIED: env] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| 静态三 CEP + 出口门控（锁定） | 运行时动态编译 Pattern / 商业 CEP | 被 STACK/FUT/CONTEXT 明确拒绝 |
| 出口门控过滤 | 用 Broadcast 在 CEP 前过滤事件 | 无法按模式关闭输出且仍保留并行演示；门控更贴「选择预编译模式集」叙事 |
| `BroadcastProcessFunction`（非 keyed） | `keyBy(vin)` + `KeyedBroadcastProcessFunction` | 门控无需 keyed state；keyed 更贴 e03-C7 教学。**推荐前者作出口门控**（更简单）；若要强化与 C7 同构可改后者 |
| CH `ADD COLUMN IF NOT EXISTS` | 删表重建 | 学习环境可接受重建，但幂等迁移更贴合 `p03-init` 可重复执行 |
| 控制消息带 version | 仅靠到达顺序覆盖 | Flink 官方：广播元素跨 task **到达顺序可能不一致**——无 version 会破坏确定性 [CITED: Broadcast State docs] |

**Installation:**

```bash
# 基座 + p03（已有）
cd docker && make up && make init && make up-p03

# 作业（改造后）
cd projects/p03-vehicle-monitoring && mvn -q clean package && make submit
# 控制面
uv run scripts/gen_vehicle_events.py --publish-control '{"activePatterns":["TRIPLE_HARSH"],"version":2}'
uv run scripts/gen_vehicle_events.py --scenario match-triple-harsh
bash scripts/verify.sh   # 或带 PATTERN_ID 环境变量的扩展断言
```

**Version verification:**
- `flink.version=2.2.1`、`flink-cep` 同版 — `projects/p03-vehicle-monitoring/pom.xml` [VERIFIED]
- `AfterMatchSkipStrategy.skipPastLastEvent()` / `Pattern.begin(String, AfterMatchSkipStrategy)` — `javap` on `flink-cep-2.2.1.jar` [VERIFIED]
- `Pattern.within(Duration)` — Flink 2.2 CEP docs [CITED: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/libs/cep/]
- Broadcast 读写纪律 — Flink 2.2 Broadcast State docs [CITED: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/broadcast_state/]
- `confluent-kafka` 2.15.0 — `pip index versions` + slopcheck [OK] [VERIFIED]

## Package Legitimacy Audit

> 本 Phase **不引入新的 Maven/npm 坐标**；仅复用 SSOT 已锁定的 Flink/Kafka/CH 栈，Python 侧继续 `confluent-kafka`。

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| org.apache.flink:flink-cep:2.2.1 | Maven Central | 随 Flink 发行 | N/A（Apache） | github.com/apache/flink | N/A（非 PyPI） | Approved — SSOT [VERIFIED: pom + local jar] |
| org.apache.flink:flink-connector-kafka:5.0.0-2.2 | Maven Central | 官方连接器线 | N/A | github.com/apache/flink-connector-kafka | N/A | Approved — SSOT |
| confluent-kafka | PyPI | 多年（≥2.5 脚本下限；本机 2.15.0） | 高 | github.com/confluentinc/confluent-kafka-python | [OK] | Approved |
| junit-jupiter:5.10.2 | Maven Central | 稳定 | N/A | junit-team | N/A | Approved — 已用 |

**Packages removed due to slopcheck [SLOP] verdict:** none  
**Packages flagged as suspicious [SUS]:** none  

*Maven 坐标未走 slopcheck（工具面向 PyPI/npm）；合法性依据为 Apache 官方发行 + 仓库 SSOT。*

## Architecture Patterns

### System Architecture Diagram

```text
[gen_vehicle_events.py]
  |--scenario match-* ----------> Kafka vehicle.events
  |--publish-control JSON ------> Kafka vehicle.pattern.control
                                           |
                                    Flink VehicleAlertJob
                                           |
                    KafkaSource(events) → Parse → WM(ooo=5s,idle=30s)
                                           |
                                    keyBy(vin)
                          ┌────────────────┼────────────────┐
                          v                v                v
                   CEP HARSH_THEN_FAULT  TRIPLE_HARSH    DTC_PAIR
                   within(30s)           within(20s)     within(15s)
                   followedBy            times(3).consec skipPastLast
                          \                |                /
                           \     Handler→AlertEvent(+patternId)
                            \    MATCH main + TIMEOUT side
                             \______________|______________/
                                            v
                                      union allAlerts
                                            |
                         KafkaSource(control) → broadcast(ACTIVE_DESC)
                                            |
                         allAlerts.connect(controlBroadcast)
                           → PatternActivationGate
                              processBroadcastElement: 写 activePatterns（version 单调）
                              processElement: 仅当 patternId ∈ active 才 emit
                              空状态 → 默认 ["HARSH_THEN_FAULT"]
                                            |
                              ┌─────────────┴─────────────┐
                              v                           v
                     Kafka vehicle.alerts        ClickHouse vehicle_alerts
                                                 (含 pattern_id)
                              ^
                              |
                     [verify.sh] CH count WHERE pattern_id=? AND alert_type='MATCH'
                                 失败 → exit ≠ 0；Kafka 仅诊断
```

### Recommended Project Structure

```text
projects/p03-vehicle-monitoring/
├── docs/
│   └── PATTERN-LIBRARY.md          # 三条五元组 + 评审清单（VEH-03）
├── sql/
│   └── clickhouse_alerts.sql       # CREATE + ALTER ADD pattern_id
├── scripts/
│   ├── gen_vehicle_events.py       # scenarios + --publish-control
│   └── verify.sh                   # 支持 PATTERN_ID / 切换剧本断言
├── src/main/java/.../p03/
│   ├── VehicleAlertJob.java        # 三 CEP + union + 门控 + 双写
│   ├── JobConfig.java              # + controlTopic 默认 vehicle.pattern.control
│   ├── ParseVehicleJson.java       # 白名单不变
│   ├── model/
│   │   ├── VehicleEvent.java
│   │   ├── AlertEvent.java         # + patternId
│   │   └── PatternControlMessage.java  # activePatterns + version
│   ├── cep/
│   │   ├── PatternIds.java         # 常量三 ID
│   │   ├── PatternRegistry.java    # id → factory；枚举全库
│   │   ├── HarshThenFaultPattern.java
│   │   ├── TripleHarshPattern.java
│   │   ├── DtcPairPattern.java
│   │   ├── HarshThenFaultHandler.java  # 或参数化 Handler 工厂
│   │   ├── TripleHarshHandler.java
│   │   ├── DtcPairHandler.java
│   │   └── PatternActivationGate.java  # BroadcastProcessFunction
│   └── sink/ClickHouseAlertSink.java   # INSERT 含 pattern_id
└── src/test/java/.../p03/cep/
    ├── PatternRegistryWithinTest.java  # 每条 getWindowSize 非空（D-11）
    ├── HarshThenFaultPatternTest.java  # 保留/扩展
    ├── TripleHarshPatternTest.java
    ├── DtcPairPatternTest.java
    └── PatternActivationGateTest.java  # 默认集 / version / 过滤
```

Docker 增量：

```text
docker/docker-compose.yml  p03-init command:
  + kafka-topics --create --if-not-exists --topic vehicle.pattern.control ...
```

### Pattern 1: 模式注册表 + 五元组文档（VEH-03）

**What:** 代码侧 `PatternRegistry.all()` 列出三工厂；文档侧 `PATTERN-LIBRARY.md` 逐条登记五元组；单测对每条断言 `pattern.getWindowSize().isPresent()`。  
**When to use:** 任何合入 CEP 模式的变更。  
**Example five-tuple rows（写进文档，非散文）：**

| patternId | 业务含义 | within | 连接语义 | skip | 状态上界论证 |
|-----------|----------|--------|----------|------|--------------|
| HARSH_THEN_FAULT | 急加速后短时故障 | 30s | followedBy（可穿插 HEARTBEAT） | 默认 noSkip | 单 vin 进行中部分匹配：已捕获 harsh 等待 fault 的分支数 ≤ 入窗事件数；`within` 为 TTL，水位推进后超时释放 |
| TRIPLE_HARSH | 20s 内连续 3 次急加速 | 20s | times(3).consecutive()（中间非匹配打断） | 默认 noSkip（量词内部 consecutive） | 进行中部分匹配长度 ≤2；`within(20s)` 清理；造数禁止在三次 HARSH 间插 HEARTBEAT |
| DTC_PAIR | 15s 内两次 DTC | 15s | followedBy | **skipPastLastEvent** | 匹配后丢弃重叠部分匹配，抑制 DTC 滑动窗口式重复告警；`within` 限制等待第二 DTC 的寿命 |

### Pattern 2: 静态多 CEP + Broadcast 出口门控（VEH-04）

**What:** 编译期固定 3 条 Pattern；运行期只改「哪些 patternId 允许落库」。  
**When to use:** 开源 Flink CEP 动态化（e10 README / docs/10-cep 明确路线）。  
**Why not disable CEP branches:** Flink 无法在不重启下热插拔算子；门控是学习工程标准答案。**代价（必须写入踩坑）：** 未激活模式仍占用 CEP 状态——仅抑制输出。三模式 MVP 可接受。

### Pattern 3: Broadcast 确定性 + version 单调

**What:** `processBroadcastElement` 解析 JSON → 仅当 `version > storedVersion` 时写入 MapState；`processElement` 只读判断 `activePatterns.contains(alert.patternId)`；state 为空时用默认 `HARSH_THEN_FAULT`。  
**When to use:** 所有控制面更新。  
**Source:** Flink Broadcast State「跨 task 到达顺序可能不同 / processBroadcastElement 必须确定性」[CITED: Broadcast State docs]。

### Anti-Patterns to Avoid

- **运行时拼 Pattern 字符串 / ScriptEngine 编译：** 违反 D-07 / STACK
- **在 `processElement` 写 Broadcast State：** API 只读；破坏一致性
- **控制更新依赖 `System.currentTimeMillis()` 或随机：** 破坏跨 task 一致性（D-05）
- **无 version 的 last-write-wins：** 广播乱序导致并行实例激活集分叉
- **无 within 的新模式合入：** D-11 / docs/10-cep 红线
- **扩展 signalType 白名单：** D-01 禁止
- **verify 以 Kafka 放行：** Phase 1 已否决；CH 唯一权威
- **切换模式后不 TRUNCATE / 不按 pattern_id 过滤：** 旧行污染断言

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CEP NFA / within / skip / times | 自写状态机 + Timer | flink-cep Pattern API | 部分匹配、水位联动、skip 剪枝已处理 [CITED: CEP docs] |
| 动态规则分发 | 文件轮询 / REST 轮询改本地缓存 | Kafka + Broadcast State（e03-C7） | 与 checkpoint 一致；教学同构 |
| 动态编译 Pattern | Groovy/JS 规则引擎 | 预编译模式集 + 门控 | STACK 拒绝商业动态 CEP |
| within 门禁 | 仅文档 checklist | `Pattern.getWindowSize()` 单测 + Registry | Phase 1 Wave 0 已证明可自动化 |
| CH 权威断言 | 自定义「作业指标即验收」 | `verify.sh` + CH SQL | Core Value / D-10 |

**Key insight:** 本 Phase 价值是「可评审模式库 + 可演示的开源动态化路线」，不是缩小 CEP 状态或实现真正的热插拔 NFA。

## Common Pitfalls

### Pitfall 1: Watermark 未推进 → 新模式「永不 MATCH」
**What goes wrong:** `TRIPLE_HARSH` / `DTC_PAIR` 造数后 CH 无行。  
**Why it happens:** Phase 1 已踩：BoundedOutOfOrderness(5s) 下水位停在 maxTs−5s；无尾心跳则 CEP 输出延迟。  
**How to avoid:** 每个 scenario 在末事件后追加 +12s/+18s HEARTBEAT（或等价推进）；文档写明。  
**Warning signs:** Flink UI 有 records in、无 out；verify 超时。

### Pitfall 2: `consecutive()` 被 HEARTBEAT 打断
**What goes wrong:** `match-triple-harsh` 永远不命中。  
**Why it happens:** `times(3).consecutive()` 要求三次匹配事件严格连续，中间非匹配事件（HEARTBEAT）打断 [CITED: CEP consecutive + e10-C1]。  
**How to avoid:** 造数连续发 3× `HARSH_ACCEL`（value>450），心跳只放在序列前后。  
**Warning signs:** 单测谓词过、e2e 不过。

### Pitfall 3: Broadcast 激活集跨并行实例不一致
**What goes wrong:** 部分 subtask 仍按旧模式放行。  
**Why it happens:** 广播到达顺序不一致；或写入用了本地时钟/随机。  
**How to avoid:** JSON 自带单调 `version`；仅 `version > current` 才更新；禁止非确定性逻辑（D-05）。  
**Warning signs:** 同 vin 重复告警 pattern_id 混乱；重跑不稳定。

### Pitfall 4: 门控关闭 ≠ CEP 状态停止增长
**What goes wrong:** 维护者以为「关掉模式」即释放状态，长时间压测内存仍涨。  
**Why it happens:** D-07 静态图始终跑满三分支。  
**How to avoid:** README/PATTERN-LIBRARY 明确写清；Phase 3 压测再量化。本 Phase 三模式 + within TTL 可接受。  
**Warning signs:** RocksDB/TM 内存随未激活模式事件持续上升。

### Pitfall 5: CH 加列与 Sink INSERT 不同步
**What goes wrong:** Sink 写 `pattern_id` 但表无列 → HTTP 非 2xx → 作业失败。  
**Why it happens:** 只改 Java 未改 `clickhouse_alerts.sql` / 未对已有表 ALTER。  
**How to avoid:** DDL 同时提供 `CREATE TABLE IF NOT EXISTS ... pattern_id` 与 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS pattern_id`；`make up-p03` 重跑幂等。  
**Warning signs:** Sink 日志非 2xx；CH 无新行。

### Pitfall 6: 默认激活集破坏 Phase 1 回归
**What goes wrong:** 未发控制消息时 `match`/`match-harsh-fault` 验不过。  
**Why it happens:** 门控空状态当作「全部拒绝」或默认空集。  
**How to avoid:** D-06 — 空状态 ≡ `["HARSH_THEN_FAULT"]`；单测覆盖。  
**Warning signs:** 旧 Makefile `gen` 路径变红。

### Pitfall 7: Handler 硬编码步骤名导致第三模式崩溃
**What goes wrong:** 复用 `AlertPatternHandler` 读 `match.get("harsh")` 用于 DTC_PAIR → NPE。  
**Why it happens:** Phase 1 Handler 与模式步骤名耦合。  
**How to avoid:** 每模式独立 Handler（或工厂注入步骤名/patternId）；TIMEOUT 同样打 patternId。  
**Warning signs:** TaskManager 异常；TIMEOUT 无 patternId。

### Pitfall 8: skip 策略未挂到 `begin`
**What goes wrong:** `DTC_PAIR` 文档写了 skipPastLastEvent，代码却默认 noSkip，重叠告警暴增。  
**Why it happens:** API 要求 `Pattern.begin("name", AfterMatchSkipStrategy.skipPastLastEvent())` [CITED: CEP docs L619–624]。  
**How to avoid:** 单测可反射/文档+代码评审；e2e 用 3 个连续 DTC 断言 MATCH 次数上界。  
**Warning signs:** 同 vin 短时大量 DTC_PAIR MATCH。

## Code Examples

### 三模式工厂（示意）

```java
// Source: Flink 2.2 CEP docs + examples/e10-cep C1/C5 + javap AfterMatchSkipStrategy
// HARSH_THEN_FAULT — 已有 HarshThenFaultPattern.build()

// TRIPLE_HARSH — 对齐 e10-C1
Pattern.<VehicleEvent>begin("harsh")
    .where(SimpleCondition.of(e ->
        "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
    .times(3).consecutive()
    .within(Duration.ofSeconds(20));

// DTC_PAIR — skip 必须传入 begin
Pattern.<VehicleEvent>begin("dtc1", AfterMatchSkipStrategy.skipPastLastEvent())
    .where(SimpleCondition.of(e -> "DTC".equals(e.signalType) && e.value > 480))
    .followedBy("dtc2")
    .where(SimpleCondition.of(e -> "DTC".equals(e.signalType) && e.value > 480))
    .within(Duration.ofSeconds(15));
```

### 作业图接线（示意）

```java
// Source: 仓库 VehicleAlertJob + e03-C7 + Flink Broadcast State docs
DataStream<VehicleEvent> keyedEvents = events.keyBy(e -> e.vin); // 实际：各 CEP 各自 keyBy

var htf = CEP.pattern(events.keyBy(e -> e.vin), HarshThenFaultPattern.build())
    .process(new HarshThenFaultHandler())
    .uid("p03-cep-harsh-then-fault");
var triple = CEP.pattern(events.keyBy(e -> e.vin), TripleHarshPattern.build())
    .process(new TripleHarshHandler())
    .uid("p03-cep-triple-harsh");
var dtcPair = CEP.pattern(events.keyBy(e -> e.vin), DtcPairPattern.build())
    .process(new DtcPairHandler())
    .uid("p03-cep-dtc-pair");

DataStream<AlertEvent> allAlerts = htf.union(htf.getSideOutput(TIMEOUT_TAG))
    .union(triple).union(triple.getSideOutput(TIMEOUT_TAG))
    .union(dtcPair).union(dtcPair.getSideOutput(TIMEOUT_TAG));

MapStateDescriptor<String, PatternControlMessage> ACTIVE_DESC =
    new MapStateDescriptor<>("p03-active-patterns", String.class, PatternControlMessage.class);

BroadcastStream<PatternControlMessage> controlBroadcast = env
    .fromSource(controlSource, WatermarkStrategy.noWatermarks(), "pattern-control")
    .uid("p03-source-pattern-control")
    .flatMap(new ParsePatternControlJson())
    .broadcast(ACTIVE_DESC);

DataStream<AlertEvent> gated = allAlerts
    .connect(controlBroadcast)
    .process(new PatternActivationGate(ACTIVE_DESC)) // BroadcastProcessFunction
    .uid("p03-gate-pattern-activation");
// gated → 现有 Kafka + ClickHouse sinks
```

### 门控核心逻辑（示意）

```java
// Source: Flink Broadcast State docs — processBroadcastElement 可写；processElement 只读
static final String STATE_KEY = "active";
static final Set<String> DEFAULT_ACTIVE = Set.of("HARSH_THEN_FAULT");

@Override
public void processBroadcastElement(PatternControlMessage msg, Context ctx, Collector<AlertEvent> out)
        throws Exception {
    var state = ctx.getBroadcastState(ACTIVE_DESC);
    PatternControlMessage cur = state.get(STATE_KEY);
    long curVer = cur == null ? -1L : cur.version;
    if (msg.version > curVer) {          // 单调 version，抗乱序
        state.put(STATE_KEY, msg);       // 内容仅来自消息本身
    }
}

@Override
public void processElement(AlertEvent alert, ReadOnlyContext ctx, Collector<AlertEvent> out)
        throws Exception {
    PatternControlMessage cur = ctx.getBroadcastState(ACTIVE_DESC).get(STATE_KEY);
    Set<String> active = cur == null || cur.activePatterns == null || cur.activePatterns.isEmpty()
            ? DEFAULT_ACTIVE : Set.copyOf(cur.activePatterns);
    if (active.contains(alert.patternId)) {
        out.collect(alert);
    }
}
```

### within 注册表单测（D-11）

```java
// Source: HarshThenFaultPatternTest.getWindowSize 模式（Phase 1 Wave 0）
@Test
void everyRegisteredPatternRequiresWithin() {
    for (var entry : PatternRegistry.all()) {
        assertTrue(entry.pattern().getWindowSize().isPresent(),
            entry.id() + " 必须 within（docs/10-cep / D-11）");
    }
    assertEquals(3, PatternRegistry.all().size());
}
```

### 控制消息与 CH 断言

```bash
# 控制面（宿主机 EXTERNAL listener）
uv run scripts/gen_vehicle_events.py \
  --publish-control '{"activePatterns":["TRIPLE_HARSH"],"version":2}'

# 权威断言（唯一 exit 0 条件）
SELECT count() FROM flinklab.vehicle_alerts
WHERE alert_type='MATCH' AND pattern_id='TRIPLE_HARSH'
```

### CH DDL 迁移（幂等）

```sql
-- Source: ClickHouse 24.x ADD COLUMN IF NOT EXISTS；挂入 p03-init 同一 sql 文件
CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts ( ... , pattern_id String DEFAULT '' , ... );

ALTER TABLE flinklab.vehicle_alerts
    ADD COLUMN IF NOT EXISTS pattern_id String DEFAULT '';
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| 单模式 HarshThenFault 硬编码 | PatternRegistry + 3 预编译分支 | Phase 2 | VEH-03 |
| 重启作业换规则 | Broadcast 选激活集 | Phase 2 / e10 预告 | VEH-04；不重启 |
| 商业动态 CEP | 开源「预编译 + Broadcast」 | STACK 明确拒绝商业路径 | 学习工程可复现 |
| 告警无 pattern 维度 | AlertEvent.patternId + CH 列 | Phase 2 | 可断言切换 |

**Deprecated/outdated:**
- 文件热读 / 作业参数作为验收主路径换模式 — D-04 禁止
- 无 within 模式「先上线再补窗口」— docs/10-cep 红线

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | 出口门控用非 keyed `BroadcastProcessFunction` 足够（无需 keyed state） | Discretion / Architecture | 若教学强绑定 KeyedBroadcastProcessFunction，改 `keyBy(vin)` 即可，行为等价 |
| A2 | CH `ADD COLUMN IF NOT EXISTS` 在 24.8 幂等可用 | Pitfall 5 | 若环境拒绝 ALTER，可文档要求 `DROP TABLE` 后重跑 init（学习环境可接受） |
| A3 | `harsh_value`/`fault_value` 列可复用于非 harsh→fault 模式（语义由 pattern_id+message 解释） | Schema | 若评审要求改列名，属额外迁移，扩大范围 |
| A4 | 默认 within 秒数 30/20/15 与阈值沿用 Phase 1 / CONTEXT，造数可稳定触发 | D-02 Discretion | 实现期可微调，但必须同步五元组与 docs |

**若需用户确认：** A1（门控是否必须 Keyed 同构）可选；其余不阻塞规划。

## Open Questions

1. **门控算子形态（Discretion — 研究员推荐已给出）**
   - What we know: 非 keyed 与 keyed 门控均可满足 D-04–D-09。
   - What's unclear: 无 — 推荐 `BroadcastProcessFunction`；planner 可直接采纳。
   - Recommendation: 采用推荐；SUMMARY 记一笔教学取舍即可。

2. **Phase 1 Makefile `gen` 场景名兼容**
   - What we know: 现为 `--scenario match`。
   - What's unclear: 是否保留 `match` 别名指向 `match-harsh-fault`。
   - Recommendation: **保留 `match` 作为别名**，避免破坏既有 README 命令；新名为主。

3. **verify 脚本接口**
   - What we know: 现硬编码 MATCH count≥1。
   - What's unclear: 单脚本多模式 vs 多脚本。
   - Recommendation: 扩展 `PATTERN_ID`（默认 `HARSH_THEN_FAULT`）+ 可选 `MIN_COUNT`；切换剧本用 Makefile 目标 `verify-switch` 串联 TRUNCATE→control→gen→assert。

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker / Compose / OrbStack | e2e / p03-init | ✓ | Docker 29.4.0；Compose v5.1.2；Context=orbstack；arch=arm64 | — |
| JDK 21 | 编译 | ✓ | openjdk 21.0.2 | — |
| Maven 3.9+ | package / surefire | ✓ | 3.9.14 | — |
| uv + confluent-kafka | 造数/控制消息 | ✓ | uv 0.10.10；confluent-kafka 2.15.0 | — |
| Flink/Kafka/CH 基座 | e2e | ✓（需 `make up`） | SSOT | — |
| flink-cep 2.2.1 local jar | API 核实 | ✓ | ~/.m2/.../flink-cep-2.2.1.jar | — |
| Context7 MCP | 文档检索 | ✗ | — | 官方 nightlies + javap + 仓库 Demo |
| graphify | 交叉引用 | ✗（disabled） | — | CONTEXT canonical_refs |

**Missing dependencies with no fallback:**
- 无阻塞项。

**Missing dependencies with fallback:**
- Context7 → Flink 2.2 官方 CEP / Broadcast 文档 + 本地 jar `javap`
- graphify → 人工 canonical_refs

## Validation Architecture

> `workflow.nyquist_validation: true` — 本节供后续 VALIDATION.md 派生。

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit Jupiter 5.10.2 + maven-surefire-plugin 3.2.5（p03 pom 已配）；e2e：bash `verify.sh` |
| Config file | `projects/p03-vehicle-monitoring/pom.xml`（surefire） |
| Quick run command | `cd projects/p03-vehicle-monitoring && mvn -q test` |
| Full suite command | OrbStack：`make up-p03` → `make submit` → control/gen → `bash scripts/verify.sh`（按 pattern_id）；另 `bash scripts/qa_check.sh` |
| 仓库门禁 | `bash scripts/qa_check.sh` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VEH-03 | 恰好 3 条模式在 Registry | unit | `mvn -q -Dtest=PatternRegistryWithinTest test` | ❌ Wave 0 |
| VEH-03 | 每条 Pattern 含 within | unit | 同上（`getWindowSize()`） | ❌ Wave 0（扩展现有 HarshThenFault 测法） |
| VEH-03 | PATTERN-LIBRARY.md 五元组齐全 | docs gate / checklist | 人工或 `rg` 检查三 ID + within/skip 关键字；评审清单勾选 | ❌ Wave 0 |
| VEH-03 | TRIPLE_HARSH / DTC_PAIR 工厂可构建 | unit | `TripleHarshPatternTest` / `DtcPairPatternTest` | ❌ Wave 0 |
| VEH-04 | 默认激活 HARSH_THEN_FAULT | unit | `PatternActivationGateTest` 空 state 放行 HTF、拒绝其他 | ❌ Wave 0 |
| VEH-04 | version 单调更新 | unit | 低 version 不覆盖高 version | ❌ Wave 0 |
| VEH-04 | 控制消息切换后 CH 出现目标 pattern_id | e2e | `verify.sh` + `PATTERN_ID=TRIPLE_HARSH`（先 TRUNCATE） | ❌ 扩展现有 verify |
| VEH-04 | 未激活模式 MATCH 不落库 | e2e | 激活仅 TRIPLE → 跑 match-harsh-fault → HTF count 不增 | ❌ Wave 0 |
| VEH-04 | control topic 由 p03-init 创建 | smoke | `kafka-topics --list` 含 `vehicle.pattern.control` | ❌ |
| ENG / 回归 | Phase 1 默认路径仍绿 | e2e | 不发 control → `match-harsh-fault`（或 `match` 别名）→ verify 默认 PATTERN_ID | ✅ 需保持 |
| ENG | qa_check | gate | `bash scripts/qa_check.sh` | ✅ |

### Sampling Rate
- **Per task commit:** `mvn -q test`（p03 模块）
- **Per wave merge:** 至少一条切换剧本 e2e（TRUNCATE → control → gen → verify PATTERN_ID）
- **Phase gate:** 三模式文档五元组齐全 + within 单测绿 + 切换 e2e 绿 + `qa_check.sh` 绿 + 默认路径回归绿

### Wave 0 Gaps
- [ ] `PatternRegistryWithinTest.java` — VEH-03 / D-11
- [ ] `TripleHarshPatternTest.java` / `DtcPairPatternTest.java` — 谓词 + within +（DTC）skip 挂载可观察性
- [ ] `PatternActivationGateTest.java` — 默认集 / version / 过滤
- [ ] `verify.sh` 支持 `PATTERN_ID`（默认 `HARSH_THEN_FAULT`）
- [ ] `gen_vehicle_events.py` scenarios + `--publish-control`
- [ ] `clickhouse_alerts.sql` pattern_id + p03-init control topic
- [ ] `docs/PATTERN-LIBRARY.md` + README 交叉引用 + 评审清单（五元组缺项即失败）

## Security Domain

> `security_enforcement: true`（config.json）；ASVS Level 1。

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | 本地 lab，无 IdP |
| V3 Session Management | no | — |
| V4 Access Control | partial | Compose 本地网络；control topic 无 ACL（学习环境接受）；勿对公网暴露 |
| V5 Input Validation | yes | 控制 JSON 白名单 patternId；非法 ID 丢弃/忽略；AlertEvent.patternId 校验；CH 字段拒引号反斜杠（沿用 Sink） |
| V6 Cryptography | no | PLAINTEXT lab；文档标明非生产 |

### Known Threat Patterns for CEP + Broadcast control

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| 恶意控制消息注入任意 patternId / 超大数组 | Tampering / DoS | 解析后与 `PatternIds` 白名单求交；限制 activePatterns 长度≤3；坏消息跳过 |
| CH INSERT 拼接注入 | Tampering | 沿用 `ClickHouseAlertSink.validate`；patternId 同样禁引号反斜杠 |
| 脏事件撑爆三路 CEP 状态 | Denial of Service | 强制 within；Parse 白名单；本 Phase 不扩 signalType |
| 凭证进 git | Information Disclosure | 不新增密钥；沿用 docker/.env |
| 控制面无认证被局域网误触 | Elevation | 文档标明 lab 限制；Phase 范围不做 ACL |

## Sources

### Primary (HIGH confidence)
- `.planning/phases/02-p03-broadcast/02-CONTEXT.md` — 锁定决策 D-01–D-12
- 仓库代码：`VehicleAlertJob` / `HarshThenFaultPattern` / `AlertPatternHandler` / `verify.sh` / `gen_vehicle_events.py` / `clickhouse_alerts.sql` / `C7BroadcastRuleJob` / e10-C1 / e10 README / docs/10-cep / docs/03-state §03-02
- [CITED: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/libs/cep/] — within、times/consecutive、AfterMatchSkipStrategy、TimedOutPartialMatchHandler
- [CITED: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/broadcast_state/] — 读写纪律、确定性、到达顺序、内存态
- [VERIFIED: javap flink-cep-2.2.1.jar] — `AfterMatchSkipStrategy.skipPastLastEvent()`、`Pattern.begin(String, AfterMatchSkipStrategy)`、`within(Duration)`
- [VERIFIED: p03 pom + env] — Flink 2.2.1、JUnit、Docker OrbStack arm64

### Secondary (MEDIUM confidence)
- `.planning/phases/01-p03/01-RESEARCH.md` / `01-02-SUMMARY` / `01-03-SUMMARY` — WM 尾心跳、CH 权威、独立 pom
- `.planning/research/STACK.md` / `ARCHITECTURE.md` — 拒绝商业动态 CEP；构建顺序第 2 步
- e12-17 StreamingGuardrailJob — Broadcast 热更新教学同构

### Tertiary (LOW confidence)
- 未激活分支的长期内存曲线定量 — 留 Phase 3 压测；本 Phase 仅定性警示

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — SSOT + 本地 jar API + 官方 2.2 文档
- Architecture: HIGH — CONTEXT 锁定静态图+门控；与 e10/e03 叙事一致
- Pitfalls: HIGH — Phase 1 e2e 实踩（WM/S3a）+ Flink Broadcast 官方乱序警告 + consecutive 语义

**Research date:** 2026-07-18  
**Valid until:** 2026-08-17（Flink 2.2.1 稳定线；若升连接器/CEP API 需重验）
