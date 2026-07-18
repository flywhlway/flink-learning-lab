# Phase 2: p03 模式库与 Broadcast - Pattern Map

**Mapped:** 2026-07-18
**Files analyzed:** 22
**Analogs found:** 19 / 22

> 本 Phase 在 Phase 1 已交付的 `projects/p03-vehicle-monitoring` 上演进。优先复用 **同项目自身文件**；Broadcast / `times(3).consecutive()` 机制分别对齐 e03-C7、e10-C1。

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `cep/PatternIds.java` | utility | transform | 常量风格见 `JobConfig` 默认 topic 字符串 | role-match |
| `cep/PatternRegistry.java` | service | event-driven | `HarshThenFaultPattern` + RESEARCH 注册表示意 | partial |
| `cep/HarshThenFaultPattern.java` | service | event-driven | **自身**（保留） | exact |
| `cep/TripleHarshPattern.java` | service | event-driven | `examples/e10-cep/.../C1TripleHighSpendJob.java` | exact |
| `cep/DtcPairPattern.java` | service | event-driven | `HarshThenFaultPattern` + docs skip 说明 | role-match |
| `cep/AlertPatternHandler.java`（或每模式 Handler） | service | event-driven | **自身** `AlertPatternHandler` | exact |
| `cep/PatternActivationGate.java` | service | event-driven | `e03/.../C7BroadcastRuleJob.java`（读写纪律） | role-match |
| `model/PatternControlMessage.java` | model | transform | `C7BroadcastRuleJob.Rule` / `GuardrailRule` | exact |
| `model/AlertEvent.java`（+`patternId`） | model | transform | **自身** `AlertEvent` | exact |
| `JobConfig.java`（+`controlTopic`） | config | request-response | **自身** `JobConfig` | exact |
| `VehicleAlertJob.java`（三 CEP + union + 门控） | service | streaming | **自身** + C7 `connect`/`broadcast` | exact |
| `sink/ClickHouseAlertSink.java`（INSERT `pattern_id`） | service | streaming | **自身** `ClickHouseAlertSink` | exact |
| `sql/clickhouse_alerts.sql`（+`pattern_id` / ALTER） | migration | file-I/O | **自身** DDL + RESEARCH ALTER | exact |
| `docker/docker-compose.yml`（`vehicle.pattern.control`） | config | batch | 同文件 `p03-init` topic 创建块 | exact |
| `scripts/gen_vehicle_events.py`（scenarios + control） | utility | pub-sub | **自身** + `emit`/尾心跳纪律 | exact |
| `scripts/verify.sh`（`PATTERN_ID`） | test | request-response | **自身** CH 权威断言 | exact |
| `docs/PATTERN-LIBRARY.md` | config | file-I/O | `examples/e10-cep/README.md` 五元组模板 | exact |
| `README.md`（交叉引用 PATTERN-LIBRARY） | config | file-I/O | **自身** README + e10 八段式 | role-match |
| `PatternRegistryWithinTest.java` | test | event-driven | `HarshThenFaultPatternTest.patternRequiresWithinThirtySeconds` | exact |
| `TripleHarshPatternTest.java` | test | event-driven | `HarshThenFaultPatternTest` | exact |
| `DtcPairPatternTest.java` | test | event-driven | `HarshThenFaultPatternTest` | exact |
| `PatternActivationGateTest.java` | test | event-driven | `HarshThenFaultPatternTest` 桩风格 + RESEARCH 门控逻辑 | partial |

**Explicitly unchanged (do not rewrite):** `ParseVehicleJson.java`（D-01 白名单零扩展）、`model/VehicleEvent.java`。

## Pattern Assignments

### `cep/TripleHarshPattern.java`（service, event-driven）

**Analog:** `examples/e10-cep/src/main/java/com/flywhl/flinklab/e10/C1TripleHighSpendJob.java`

**Core `times(3).consecutive()` + within** (lines 38–41):
```java
Pattern<Event, ?> pattern = Pattern.<Event>begin("high")
        .where(SimpleCondition.of(e -> e.amount > 400))
        .times(3).consecutive()
        .within(Duration.ofSeconds(20));
```

**Apply to p03:**
```java
Pattern.<VehicleEvent>begin("harsh")
    .where(SimpleCondition.of(e ->
        "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
    .times(3).consecutive()
    .within(Duration.ofSeconds(20));
```

**造数红线（e10-C1 / RESEARCH Pitfall 2）：** 三次 HARSH 之间**禁止**插 HEARTBEAT；心跳只放序列前后 + 尾心跳推进 WM。

---

### `cep/DtcPairPattern.java`（service, event-driven）

**Analogs:**
1. 工厂形态：`HarshThenFaultPattern.build()`（followedBy + within）
2. skip 语义：**仓库无 Java 样例** — 用 Flink API + docs/10-cep / RESEARCH 示意

**followedBy + within 骨架** (`HarshThenFaultPattern.java` lines 22–29):
```java
return Pattern.<VehicleEvent>begin("harsh")
        .where(SimpleCondition.of(
                e -> "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
        .followedBy("fault")
        .where(SimpleCondition.of(
                e -> "DTC".equals(e.signalType) && e.value > 480))
        .within(Duration.ofSeconds(30));
```

**Apply — skip 必须挂到 `begin`（RESEARCH / CEP docs）：**
```java
Pattern.<VehicleEvent>begin("dtc1", AfterMatchSkipStrategy.skipPastLastEvent())
    .where(SimpleCondition.of(e -> "DTC".equals(e.signalType) && e.value > 480))
    .followedBy("dtc2")
    .where(SimpleCondition.of(e -> "DTC".equals(e.signalType) && e.value > 480))
    .within(Duration.ofSeconds(15));
```

**禁止：** 文档写 skip、代码却用默认 `begin("dtc1")`（Pitfall 8）。

---

### `cep/HarshThenFaultPattern.java`（service, event-driven）

**Analog:** 自身 — Phase 1 已交付，保留为模式库第 1 条。

复制 `build()` 不变；经 `PatternRegistry` 以 id `HARSH_THEN_FAULT` 注册。

---

### `cep/PatternRegistry.java` / `PatternIds.java`（service / utility）

**Analog（partial）：** 无现成注册表类；工厂风格对齐 `HarshThenFaultPattern`；常量命名对齐 `JobConfig` 默认 topic。

**Planner 落地（RESEARCH）：**
```java
// PatternIds: HARSH_THEN_FAULT / TRIPLE_HARSH / DTC_PAIR
// PatternRegistry.all() → List<entry(id, pattern)> 恰好 3 条
// 单测：每条 getWindowSize().isPresent()；size == 3
```

---

### Handlers（`AlertPatternHandler` 演进或每模式独立）（service, event-driven）

**Analog:** `projects/p03-vehicle-monitoring/.../cep/AlertPatternHandler.java`

**MATCH + TIMEOUT Side Output** (lines 37–68):
```java
@Override
public void processMatch(Map<String, List<VehicleEvent>> match, Context ctx, Collector<AlertEvent> out) {
    VehicleEvent harsh = match.get("harsh").get(0);
    VehicleEvent fault = match.get("fault").get(0);
    out.collect(new AlertEvent(harsh.vin, "MATCH", harsh.value, fault.value,
            fault.eventTime, "急加速后出现故障信号"));
}

@Override
public void processTimedOutMatch(Map<String, List<VehicleEvent>> match, Context ctx) {
    List<VehicleEvent> harshEvents = match.get("harsh");
    if (harshEvents == null || harshEvents.isEmpty()) {
        return;
    }
    VehicleEvent harsh = harshEvents.get(0);
    ctx.output(timeoutTag, new AlertEvent(
            harsh.vin, "TIMEOUT", harsh.value, 0.0, harsh.eventTime,
            "急加速后 30s 内未出现故障信号"));
}
```

**Apply（RESEARCH Pitfall 7）：**
- 每模式独立 Handler（或工厂注入步骤名 + `patternId`）——**禁止**对 `TRIPLE_HARSH`/`DTC_PAIR` 硬读 `match.get("harsh")`/`"fault"`
- 构造 `AlertEvent` 时写入 `patternId`（MATCH 与 TIMEOUT 均带）
- `TIMEOUT_TAG` 可共享或每分支独立，但 union 后必须进同一门控

---

### `cep/PatternActivationGate.java`（service, event-driven）

**Analog:** `examples/e03-state/.../C7BroadcastRuleJob.java`（+ e12-17 同构）

> RESEARCH 推荐非 keyed `BroadcastProcessFunction`；仓库样板均为 `KeyedBroadcastProcessFunction`。**读写纪律同构**，仅去掉 `keyBy`。

**MapStateDescriptor + broadcast + 两入口** (C7 lines 49–91):
```java
private static final MapStateDescriptor<String, Rule> RULES_DESC =
        new MapStateDescriptor<>("rules", String.class, Rule.class);

BroadcastStream<Rule> rules = env
        .fromSource(...)
        .broadcast(RULES_DESC);

orders.keyBy(e -> e.userId)
      .connect(rules)
      .process(new KeyedBroadcastProcessFunction<String, Event, Rule, String>() {
          @Override
          public void processElement(Event e, ReadOnlyContext ctx, Collector<String> out)
                  throws Exception {
              ReadOnlyBroadcastState<String, Rule> st = ctx.getBroadcastState(RULES_DESC);
              Rule r = st.get(e.page);
              double th = r == null ? 490 : r.threshold; // 空状态兜底
              if (e.amount > th) {
                  out.collect(...);
              }
          }

          @Override
          public void processBroadcastElement(Rule r, Context ctx, Collector<String> out)
                  throws Exception {
              ctx.getBroadcastState(RULES_DESC).put(r.page, r); // 仅广播侧写
          }
      })
      .uid("e03-c7-broadcast");
```

**Apply to 出口门控（RESEARCH 门控核心，planner 直接用）：**
```java
// Descriptor 建议: "p03-active-patterns"
// processBroadcastElement: 仅当 msg.version > storedVersion 才 put（抗乱序）
// processElement: 只读；active.contains(alert.patternId) 才 emit
// 空状态 ≡ DEFAULT_ACTIVE = Set.of("HARSH_THEN_FAULT")  // D-06
// 内容仅来自消息本身 — 禁止 System.currentTimeMillis / 随机（D-05）
```

**接线：** `allAlerts.connect(controlBroadcast).process(gate).uid("p03-gate-pattern-activation")` — **不要**在 CEP 前用 Broadcast 过滤事件（RESEARCH Pattern 2）。

---

### `model/PatternControlMessage.java`（model, transform）

**Analog:** `C7BroadcastRuleJob.Rule` (lines 36–46) / `StreamingGuardrailJob.GuardrailRule`

```java
public static class Rule {
    public String page;
    public double threshold;
    public Rule() { }
    public Rule(String page, double threshold) {
        this.page = page;
        this.threshold = threshold;
    }
}
```

**Apply:** public 字段 + 无参构造；`List<String> activePatterns` + `long version`；JSON 对齐 D-04：`{"activePatterns":["HARSH_THEN_FAULT"],"version":N}`。解析后与 `PatternIds` 白名单求交（ASVS V5）。

---

### `model/AlertEvent.java`（model, transform）

**Analog:** 自身 — 增 `public String patternId` + 构造函数参数。

现有字段（lines 10–16）保留；`harshValue`/`faultValue` 可复用于非 harsh→fault 模式（RESEARCH A3：语义由 `patternId`+`message` 解释）。

---

### `JobConfig.java`（config, request-response）

**Analog:** 自身 `JobConfig.from` (lines 47–59)

**Apply:** 增一行默认：
```java
arg(args, "control-topic", "vehicle.pattern.control"),
```
字段 `controlTopic`；禁止在 `VehicleAlertJob` 硬编码 topic 名。

---

### `VehicleAlertJob.java`（service, streaming）

**Analogs:**
1. 管线骨架：自身 `buildPipeline`（Kafka → parse → WM → CEP → 双写）
2. Broadcast 接线：C7 `broadcast` + `connect` + `.uid(...)`

**现有单 CEP + union TIMEOUT** (lines 74–103):
```java
SingleOutputStreamOperator<AlertEvent> matched = CEP
        .pattern(events.keyBy(e -> e.vin), HarshThenFaultPattern.build())
        .process(handler)
        .name("cep-harsh-then-fault")
        .uid("p03-cep-harsh-then-fault");

DataStream<AlertEvent> timeouts = matched.getSideOutput(AlertPatternHandler.TIMEOUT_TAG);
DataStream<AlertEvent> allAlerts = matched.union(timeouts);

allAlerts.sinkTo(kafkaSink)...;
allAlerts.sinkTo(new ClickHouseAlertSink(...))...;
```

**Apply — 静态三分支 + 门控后再双写（RESEARCH 作业图）：**
1. 保留 events 源 / Parse / WM（`ooo=5s` + `idleness=30s`）不变
2. 并行三条 `CEP.pattern(events.keyBy(vin), PatternX.build()).process(HandlerX).uid("p03-cep-...")`
3. 各分支 MATCH ∪ TIMEOUT → `allAlerts`
4. 新增 `KafkaSource` → control topic → parse → `.broadcast(ACTIVE_DESC)`（`WatermarkStrategy.noWatermarks()`）
5. `gated = allAlerts.connect(controlBroadcast).process(new PatternActivationGate(...)).uid("p03-gate-pattern-activation")`
6. **仅 `gated`** 双写 Kafka + ClickHouse（未激活模式 TIMEOUT/MATCH 均不落库 — D-09）

---

### `sink/ClickHouseAlertSink.java`（service, streaming）

**Analog:** 自身 `BatchWriter.flush` (lines 60–101)

**现有 INSERT 列清单** (lines 80–82):
```java
String sql = "INSERT INTO flinklab.vehicle_alerts"
        + "(vin,alert_type,signal_summary,harsh_value,fault_value,event_time) VALUES "
        + body;
```

**Apply:**
- VALUES / 列清单增加 `pattern_id`
- `validate` 对 `patternId` 同样 `containsForbidden`（引号/反斜杠）
- 可选：白名单校验 `patternId ∈ PatternIds`

---

### `sql/clickhouse_alerts.sql`（migration, file-I/O）

**Analog:** 自身 CREATE（lines 1–13）+ RESEARCH 幂等 ALTER

**Apply:**
```sql
-- CREATE 新环境含 pattern_id
CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts ( ... , pattern_id String DEFAULT '' , ... );

-- 已有 Phase 1 表：幂等加列
ALTER TABLE flinklab.vehicle_alerts
    ADD COLUMN IF NOT EXISTS pattern_id String DEFAULT '';
```

由现有 `p03-init` `wget --post-file` 重跑即可（勿改 default `01-init.sql`）。

---

### `docker/docker-compose.yml` — p03-init 增 control topic（config, batch）

**Analog:** 同文件 `p03-init` (lines 314–318)

```yaml
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic vehicle.events --partitions 4 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic vehicle.alerts --partitions 4 --replication-factor 1
```

**Apply:** 同样 `--if-not-exists` 追加 `vehicle.pattern.control`；保持 `profiles: ["p03"]`；更新完成 echo。

---

### `scripts/gen_vehicle_events.py`（utility, pub-sub）

**Analog:** 自身 `scenario_match` + `emit` (lines 39–92)

**尾心跳 / WM 纪律**（必须延续）:
```python
# DTC@+6s；+5s ooo 下需 eventTime>=DTC+5s 才能让 watermark 越过 DTC
{"signalType": "HEARTBEAT", "eventTime": base + 12_000},
{"signalType": "HEARTBEAT", "eventTime": base + 18_000},
```

**Apply:**
- `choices` 扩：`match`（别名→`match-harsh-fault`）、`match-harsh-fault`、`match-triple-harsh`、`match-dtc-pair`
- `match-triple-harsh`：连续 3× `HARSH_ACCEL(value>450)`，中间无 HEARTBEAT
- `--publish-control`：向 `vehicle.pattern.control`（或 `--control-topic`）发 JSON；bootstrap 仍 `localhost:9094`
- 复用 `emit` / `flush` / `acks=all`

---

### `scripts/verify.sh`（test, request-response）

**Analog:** 自身 CH 权威断言 (lines 15–59)

```bash
CH_MATCH_QUERY="SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH'"
# ...
if [[ "${MATCH_COUNT}" -lt 1 ]]; then
  echo "FAIL: ..." >&2
  diag_kafka
  exit 1
fi
```

**Apply（RESEARCH Open Q3）：**
```bash
PATTERN_ID="${PATTERN_ID:-HARSH_THEN_FAULT}"
MIN_COUNT="${MIN_COUNT:-1}"
CH_MATCH_QUERY="SELECT count() FROM flinklab.vehicle_alerts
  WHERE alert_type='MATCH' AND pattern_id='${PATTERN_ID}'"
# PATTERN_ID 仅允许白名单常量（防注入）；Kafka 仍仅 diag
```

切换剧本：TRUNCATE → `--publish-control` → gen → `PATTERN_ID=... verify`（Makefile 目标可选）。

---

### `docs/PATTERN-LIBRARY.md`（config, file-I/O）

**Analog:** `examples/e10-cep/README.md` lines 33–38 + `docs/10-cep/README.md` §企业实践

```markdown
实践:每条模式登记「业务含义 / within / 连接语义 / skip 策略 / 状态上界」五元组进评审。
动态化路线:开源 CEP 的 Pattern 编译期固定;运行期换规则 = Broadcast(e03-C7)选择预编译模式集
```

**Apply:** 表格登记三行（RESEARCH Pattern 1 五元组表可直接落文档）；含评审 checklist（缺项即失败）；写明「门控关闭 ≠ CEP 状态停止」（Pitfall 4）。

---

### 单测族（`PatternRegistryWithinTest` / `TripleHarsh*` / `DtcPair*` / `PatternActivationGateTest`）

**Analog:** `HarshThenFaultPatternTest.java`（Phase 1 已存在 — 优于 01-PATTERNS「无测试」结论）

**within 门禁** (lines 39–47):
```java
@Test
void patternRequiresWithinThirtySeconds() {
    Pattern<VehicleEvent, ?> pattern = HarshThenFaultPattern.build();
    Duration window = pattern.getWindowSize()
            .orElseThrow(() -> new AssertionError(
                    "Pattern 必须设置 within(Duration.ofSeconds(30))"));
    assertEquals(Duration.ofSeconds(30), window, ...);
}
```

**Apply — Registry 扫描（RESEARCH D-11）：**
```java
for (var entry : PatternRegistry.all()) {
    assertTrue(entry.pattern().getWindowSize().isPresent(),
        entry.id() + " 必须 within");
}
assertEquals(3, PatternRegistry.all().size());
```

**Gate 单测：** 空 state → 放行 `HARSH_THEN_FAULT`、拒绝其他；低 `version` 不覆盖高 `version`（可用轻量桩或直接测静态辅助方法，不必起 MiniCluster）。

---

### `README.md`（config, file-I/O）

**Analog:** 自身 p03 README + e10 八段式交叉引用习惯

增：链接 `docs/PATTERN-LIBRARY.md`；控制面 topic / `--publish-control` / `PATTERN_ID` verify；说明静态三 CEP + 出口门控路线。

## Shared Patterns

### Broadcast 写入纪律（D-05）
**Source:** `C7BroadcastRuleJob` + docs/03-state §03-02  
**Apply to:** `PatternActivationGate`  
- 仅 `processBroadcastElement` 写 Broadcast State  
- `processElement` 只读  
- 禁止随机数 / 本地时钟写入 state（C7 规则源用随机仅因教学 demo；p03 控制消息必须确定性 + `version`）

### CEP within 红线（D-11）
**Source:** `HarshThenFaultPattern` + `HarshThenFaultPatternTest` + docs/10-cep  
**Apply to:** 全部三工厂 + `PatternRegistryWithinTest`

### TIMEOUT Side Output + 同表 alert_type
**Source:** `AlertPatternHandler` + `VehicleAlertJob` union  
**Apply to:** 三分支 TIMEOUT 均带 `patternId`，经同一门控（D-09）

### ClickHouse 唯一权威出口
**Source:** `scripts/verify.sh`  
**Apply to:** 切换验收仍以 CH `count` + `pattern_id` 为唯一 exit 0；Kafka 诊断

### Compose profile 隔离 + topic 幂等创建
**Source:** `docker-compose.yml` `p03-init`  
**Apply to:** 仅追加 `vehicle.pattern.control`；不进 default `make up`

### Kafka 双 listener
**Source:** `gen_vehicle_events.py` 注释 / `JobConfig`  
**Apply to:** 控制消息与造数：宿主机 `localhost:9094`；作业内 `kafka:9092`

### 算子 uid
**Source:** `VehicleAlertJob`  
**Apply to:** 三 CEP、control source、gate、sinks 均 `.uid("p03-...")`

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `cep/PatternRegistry.java` | service | event-driven | 仓库无「id→Pattern 工厂」注册表；结构按 RESEARCH 新建，工厂体复制各 Pattern |
| `DtcPairPattern` 的 `AfterMatchSkipStrategy` API 用法 | service | event-driven | 无 Java 样例调用 `skipPastLastEvent()`；仅有 docs/e10 README 文字；实现以 javap/RESEARCH 示意为准 |
| `PatternActivationGate` 非 keyed `BroadcastProcessFunction` | service | event-driven | 仓库仅有 `KeyedBroadcastProcessFunction`（C7/e12-17/e12-22）；纪律照抄，基类按 RESEARCH A1 选用非 keyed |

## Metadata

**Analog search scope:** `projects/p03-vehicle-monitoring/`、`examples/e03-state/`、`examples/e10-cep/`、`examples/e12-17-streaming-guardrail/`、`examples/e12-22-streaming-prompt/`、`docs/10-cep/`、`docker/docker-compose.yml`  
**Files scanned:** ~35  
**Prior map reused:** `.planning/phases/01-p03/01-PATTERNS.md`（p03 自身文件现为第一优先类比；Broadcast/C1 为机制类比）  
**Pattern extraction date:** 2026-07-18  
**Focus analogs confirmed:** p03 VehicleAlertJob/Handler/Pattern/Sink/verify/gen、e03-C7 Broadcast、e10-C1 consecutive、e10 README 五元组、HarshThenFaultPatternTest within 门禁
