# Phase 5: p02 实时推荐 - Pattern Map

**Mapped:** 2026-07-18
**Files analyzed:** 28
**Analogs found:** 26 / 28

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `projects/p02-realtime-reco/pom.xml` | config | batch | `projects/p01-log-ai-platform/pom.xml` | exact |
| `projects/p02-realtime-reco/Makefile` | config | request-response | `projects/p01-log-ai-platform/Makefile` | exact |
| `projects/p02-realtime-reco/README.md` | docs | file-I/O | `projects/p01-log-ai-platform/README.md` | exact |
| `projects/p02-realtime-reco/sql/clickhouse_reco_results.sql` | migration | file-I/O | `projects/p01-log-ai-platform/sql/clickhouse_log_results.sql` | exact |
| `projects/p02-realtime-reco/sql/postgres_reco_items.sql` | migration | file-I/O | `examples/e07-connectors` JDBC DDL 形态 + Discretion | partial |
| `projects/p02-realtime-reco/scripts/gen_reco_events.py` | utility | pub-sub | `projects/p01-log-ai-platform/scripts/gen_log_events.py` | exact |
| `projects/p02-realtime-reco/scripts/verify.sh` | test | request-response | `projects/p01-log-ai-platform/scripts/verify.sh` | exact |
| `projects/p02-realtime-reco/scripts/loadtest.sh` | utility | streaming | `projects/p01-log-ai-platform/scripts/loadtest.sh` | exact |
| `projects/p02-realtime-reco/scripts/drill_redis_degrade.sh` | test | event-driven | `projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh` | role-match |
| `projects/p02-realtime-reco/docs/ARCHITECTURE.md` | docs | file-I/O | `projects/p01-log-ai-platform/docs/ARCHITECTURE.md` | exact |
| `projects/p02-realtime-reco/docs/RESUME.md` | docs | file-I/O | `projects/p01-log-ai-platform/docs/RESUME.md` | exact |
| `projects/p02-realtime-reco/docs/baseline.md` | docs | file-I/O | `projects/p01-log-ai-platform/docs/baseline.md` | exact |
| `projects/p02-realtime-reco/docs/adr/0001-dual-channel-features.md` | docs | file-I/O | `projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md` | exact |
| `.../RealtimeRecoJob.java` | controller | streaming | `projects/p01-log-ai-platform/.../LogAiJob.java` + p03 dual-sink | exact |
| `.../JobConfig.java` | config | request-response | `projects/p01-log-ai-platform/.../JobConfig.java` | exact |
| `.../ParseBehaviorJson.java` | middleware | transform | `projects/p01-log-ai-platform/.../ParseLogJson.java` | exact |
| `.../model/BehaviorEvent.java` | model | transform | `projects/p01-log-ai-platform/.../model/LogEvent.java` | exact |
| `.../model/RecoResult.java` | model | transform | `projects/p01-log-ai-platform/.../model/LogResult.java` | exact |
| `.../model/ItemCatalog.java` | model | CRUD | —（简单 POJO；字段见 Discretion） | none |
| `.../feature/SessionFeatureFunction.java` | service | streaming | e12-06 `SessionCategoryFeature` + p01 `FeatureEnricher` | exact |
| `.../feature/RedisFeatureWriter.java` | service | streaming | e07-C7 `RedisBatchWriter`（非 e12 简化版） | exact |
| `.../catalog/CatalogLoader.java` | service | CRUD | e11-C3 `CachedEnrich.open` + JDBC `DriverManager` | role-match |
| `.../score/RuleScorer.java` | service | transform | `projects/p01-log-ai-platform/.../rule/RuleTagger.java` | role-match |
| `.../sink/ClickHouseRecoSink.java` | service | streaming | `projects/p01-log-ai-platform/.../sink/ClickHouseLogSink.java` | exact |
| `.../ParseBehaviorJsonTest.java` | test | transform | `projects/p01-log-ai-platform/.../ParseLogJsonTest.java` | exact |
| `.../RuleScorerTest.java` | test | transform | `projects/p01-log-ai-platform/.../rule/RuleTaggerTest.java` | exact |
| `.../SessionFeatureFunctionTest.java` | test | streaming | p01 FeatureEnricher 逻辑测 / 纯状态辅助 | partial |
| `docker/docker-compose.yml`（`p02-init`） | config | event-driven | `docker/docker-compose.yml` `p01-init` | exact |
| `docker/Makefile`（`up-p02`） | config | request-response | `docker/Makefile` `up-p01` | exact |
| `README.md`（jedis 矩阵行） | docs | file-I/O | `examples/pom.xml` `<jedis.version>` | role-match |
| `docs/README.md`（15-02 回填） | docs | file-I/O | 同文件 15-01/15-03 完成态行 | exact |

## Pattern Assignments

### `projects/p02-realtime-reco/pom.xml` (config, batch)

**Analog:** `projects/p01-log-ai-platform/pom.xml`

**Core pattern** (独立模块 + SSOT 属性 + shade；再追加 jedis/postgresql):

```xml
<!-- lines 7-27: 独立 groupId/artifactId；不挂 examples 父工程 -->
<groupId>com.flywhl.flinklab</groupId>
<artifactId>p01-log-ai-platform</artifactId>
<version>0.1.0</version>
<properties>
  <maven.compiler.release>21</maven.compiler.release>
  <flink.version>2.2.1</flink.version>
  <flink.kafka.connector.version>5.0.0-2.2</flink.kafka.connector.version>
  <jackson.version>2.17.2</jackson.version>
  <junit.version>5.10.2</junit.version>
</properties>
```

**Copy from examples for Redis/PG versions** (`examples/pom.xml`):

```xml
<jedis.version>5.2.0</jedis.version>
<!-- + redis.clients:jedis / org.postgresql:postgresql:42.7.4 -->
```

**Apply:** artifact → `p02-realtime-reco`；先登记根 README jedis 再写依赖。

---

### `projects/p02-realtime-reco/Makefile` (config, request-response)

**Analog:** `projects/p01-log-ai-platform/Makefile`

**Core pattern** (一键链 package → submit → gen → verify / loadtest / drill):

```makefile
# lines 1, 10-11, 66-67, 138-147
.PHONY: package test submit ... verify ... loadtest drill-degrade
package:
	mvn -q clean package
verify:
	bash scripts/verify.sh
drill-degrade:
	bash scripts/drill_ai_degrade.sh
loadtest:
	RATE=$(RATE) WARMUP_SEC=$(WARMUP_SEC) DURATION_SEC=$(DURATION_SEC) bash scripts/loadtest.sh
```

**Apply:** 目标改名 `drill-redis` → `scripts/drill_redis_degrade.sh`；主类 `RealtimeRecoJob`；job-name `p02-realtime-reco`。

---

### `RealtimeRecoJob.java` (controller, streaming)

**Analog (pipeline):** `projects/p01-log-ai-platform/.../LogAiJob.java`  
**Analog (Kafka+CH 双写):** `projects/p03-vehicle-monitoring/.../VehicleAlertJob.java`

**Imports / JobConfig / checkpoint** (p01 lines 44-56):

```java
JobConfig cfg = JobConfig.from(args);
Configuration conf = Configuration.fromMap(Map.of(
        "state.backend.type", cfg.stateBackendType,
        "execution.checkpointing.incremental", "true"));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
```

**Kafka source + parse + watermark** (p01 lines 60-78):

```java
KafkaSource<String> eventSource = KafkaSource.<String>builder()
        .setBootstrapServers(cfg.kafkaBootstrap)
        .setTopics(cfg.eventsTopic)
        .setGroupId(cfg.groupId)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
DataStream<LogEvent> events = env
        .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-log-events")
        .uid("p01-source-log-events")
        .flatMap(new ParseLogJson())
        .uid("p01-parse-log-json")
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTime)
                        .withIdleness(Duration.ofSeconds(30)));
```

**Dual sink** (p03 lines 133-151):

```java
KafkaSink<AlertEvent> kafkaSink = KafkaSink.<AlertEvent>builder()
        .setBootstrapServers(cfg.kafkaBootstrap)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(cfg.alertsTopic)
                .setValueSerializationSchema(new AlertEventJsonSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
gated.sinkTo(kafkaSink).uid("p03-sink-kafka-alerts");
gated.sinkTo(new ClickHouseAlertSink(...)).uid("p03-sink-clickhouse-alerts");
```

**p02 接线（RESEARCH Pattern 2 目标形）:**

```java
events.keyBy(e -> e.userId)
      .process(new SessionFeatureFunction())   // MapState → FeatureSnapshot
      .map(new RedisFeatureWriter(...))       // Checkpointed + jedis；失败不抛
      .uid("p02-redis-feature");
// → RuleScorer(Top-K) → Kafka reco.results + ClickHouseRecoSink
```

---

### `JobConfig.java` (config, request-response)

**Analog:** `projects/p01-log-ai-platform/.../JobConfig.java`

**Arg parsing** (lines 105-118):

```java
static String arg(String[] args, String key, String dflt) {
    String flag = "--" + key;
    for (int i = 0; i < args.length; i++) {
        String a = args[i];
        if (flag.equals(a) && i + 1 < args.length) {
            return args[i + 1];
        }
        if (a.startsWith(flag + "=")) {
            return a.substring(flag.length() + 1);
        }
    }
    return dflt;
}
```

**Defaults to copy/adapt** (lines 83-102): `kafka-bootstrap=kafka:9092`、`clickhouse-url=http://clickhouse:8123/`、手写 `--key`/`--key=value`。  
**p02 新增键：** `events-topic=reco.events`、`results-topic=reco.results`、`redis-host=redis`、`redis-port=6379`、`top-k=5`、`pg-jdbc=jdbc:postgresql://postgres:5432/flinklab`、`redis-batch-threshold`。

---

### `ParseBehaviorJson.java` (middleware, transform)

**Analog:** `projects/p01-log-ai-platform/.../ParseLogJson.java`

**Imports + FlatMap + tryParse** (lines 1-72):

```java
public final class ParseLogJson extends RichFlatMapFunction<String, LogEvent> {
    private static final Set<String> ALLOWED_LEVELS =
            Set.of("ERROR", "WARN", "INFO", "DEBUG");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String json, Collector<LogEvent> out) {
        tryParse(json).ifPresent(out::collect);
    }

    public static Optional<LogEvent> tryParse(String json) {
        // 缺字段 / 非法枚举 / 引号反斜杠 → empty，不抛
    }
}
```

**Apply:** 白名单 `VIEW|CLICK|CART|BUY`；必填 `userId`/`itemId`/`eventType`/`eventTime`（或 `ts`）；`containsForbiddenChars` 用于 userId/itemId。

---

### `model/BehaviorEvent.java` / `model/RecoResult.java` (model, transform)

**Analog:** `LogEvent.java` / `LogResult.java`

**POJO 纪律** (LogEvent lines 9-26):

```java
public final class LogEvent {
    public String service;
    public String level;
    // ...
    public long eventTime;
    public LogEvent() {}
    public LogEvent(...) { /* 全参 */ }
}
```

**RecoResult 扩展字段（对齐 D-06）:** `userId`, `itemId`（或 Top-K JSON）, `score`, `ts`, `reason`, `featureSnapshot`/`featureSource`（`REDIS`|`STATE_ONLY`）。  
**LogResult 默认枚举** (lines 21-27) → RecoResult 默认 `featureSource=STATE_ONLY` 或空，Sink 白名单校验。

---

### `feature/SessionFeatureFunction.java` (service, streaming)

**Analog (MapState):** `examples/e12-06-streaming-feature/.../StreamingFeatureJob.java` `SessionCategoryFeature`  
**Analog (Keyed 产出中间结果):** p01 `FeatureEnricher`

**MapState 核心** (e12-06 lines 74-90):

```java
public static final class SessionCategoryFeature
        extends KeyedProcessFunction<String, Event, String> {
    private transient MapState<String, Integer> categoryCount;

    @Override
    public void open(OpenContext ctx) {
        categoryCount = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("session-category", String.class, Integer.class));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
        int c = (categoryCount.contains(e.page) ? categoryCount.get(e.page) : 0) + 1;
        categoryCount.put(e.page, c);
        out.collect("feature:session_category|user=%s cat=%s cnt=%d"
                .formatted(e.userId, e.page, c));
    }
}
```

**ValueState 富化产出** (p01 FeatureEnricher lines 16-39):

```java
public final class FeatureEnricher extends KeyedProcessFunction<String, LogEvent, LogResult> {
    private transient ValueState<Long> errorCountState;
    // open → getState(ValueStateDescriptor)
    // processElement → update + 填 featureJson → out.collect
}
```

**Apply:** `keyBy(userId)`；维护 click/类目/last_ts；**输出携带 FeatureSnapshot 的中间 POJO**（供打分与 Redis 写），勿只 print String。  
**Pitfall:** 勿原样抄 e12 `ClickCountAgg`（无 userId）；用 ProcessWindowFunction 带 key，或纯 KeyedProcessFunction。

---

### `feature/RedisFeatureWriter.java` (service, streaming)

**Analog:** `examples/e07-connectors/.../C7RedisBatchWriteJob.java` `RedisBatchWriter`  
**禁止原样抄:** e12-06 `RedisFeatureWriter`（省略 Operator State）

**CheckpointedFunction + Pipeline** (e07-C7 lines 48-108):

```java
public static final class RedisBatchWriter
        extends RichMapFunction<Event, String>
        implements CheckpointedFunction {
    private final int threshold;
    private transient List<Event> buffer;
    private transient ListState<Event> checkpointed;
    private transient Jedis jedis;

    @Override
    public void open(OpenContext ctx) {
        buffer = new ArrayList<>();
        jedis = new Jedis("redis", 6379);
    }

    private String flushToRedis() {
        Pipeline p = jedis.pipelined();
        for (Event e : buffer) {
            p.set("profile:" + e.userId, "%s@%d".formatted(e.page, e.ts));
        }
        p.sync();
        buffer.clear();
        return "FLUSH ...";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        checkpointed.update(buffer);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        checkpointed = ctx.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("redis-pending", Event.class));
        if (ctx.isRestored()) {
            for (Event e : checkpointed.get()) {
                buffer.add(e);
            }
        }
    }
}
```

**p02 强制差异（D-03 / D-12）:**

1. key 形如 `feature:{userId}:click_30s` 等（非 `profile:`）。
2. `flushToRedis` **try/catch**：失败记 metric、**不抛**、主流 FeatureSnapshot 继续往下。
3. 语义文档写 at-least-once；重复 SET 幂等。

**降级语义对照** (p01 OllamaRiskAsyncFunction lines 126-131):

```java
@Override
public void timeout(LogResult input, ResultFuture<LogResult> resultFuture) {
    aiTimeouts.inc();
    aiDegrades.inc();
    resultFuture.complete(Collections.singleton(degraded(input))); // 作业不挂
}
```

---

### `catalog/CatalogLoader.java` (service, CRUD)

**Analog (open 一次加载内存):** e11-C3 `CachedEnrich.open`  
**Analog (PG JDBC 坐标):** `examples/pom.xml` postgresql + e07-C2 JDBC URL

**open 加载模式** (e11-C3 lines 60-70):

```java
@Override
public void open(OpenContext ctx) {
    lru = new LinkedHashMap<>(capacity, 0.75f, true) { ... };
    client = new FakeDimClient(60, 0);
    // p02: 此处改为 DriverManager.getConnection(jdbcUrl) → SELECT * FROM reco_items → Map
}
```

**JDBC URL 形态** (e07-C2): `jdbc:postgresql://postgres:5432/flinklab`  
**Apply:** `RichMapFunction`/`KeyedProcessFunction` 的 `open()` 调 `CatalogLoader.load(jdbcUrl)`；百级 item 全表内存打分；**不做每条 Async PG**。

---

### `score/RuleScorer.java` (service, transform)

**Analog:** `projects/p01-log-ai-platform/.../rule/RuleTagger.java`

**纯函数 + MapFunction 双入口** (lines 13-59):

```java
public final class RuleTagger implements MapFunction<LogResult, LogResult> {
    @Override
    public LogResult map(LogResult value) {
        value.ruleLabel = tag(value);
        return value;
    }
    public static String tag(LogEvent event) { ... }  // 单测入口
    public static String tag(LogResult result) { ... } // 作业入口
}
```

**Apply:**

- 静态方法 `score(FeatureSnapshot, List<Item>, K)` → Top-K `RecoResult` 列表。
- 权重：VIEW=1, CLICK=3, CART=5, BUY=10；类目 ×1.5；近因半衰期 30min。
- Redis 点查成功 → `featureSource=REDIS`；失败/超时 → 用随流 State 特征 → `STATE_ONLY`（**禁止抛异常重启**）。

---

### `sink/ClickHouseRecoSink.java` (service, streaming)

**Analog:** `projects/p01-log-ai-platform/.../sink/ClickHouseLogSink.java`

**SinkV2 + 白名单 + HTTP INSERT** (lines 22-112 核心):

```java
public final class ClickHouseLogSink implements Sink<LogResult> {
    private static final Set<String> ALLOWED_AI_SOURCES =
            Set.of("DISABLED", "AI", "RULE", "DEGRADED", "BLOCKED");

    static final class BatchWriter implements SinkWriter<LogResult> {
        @Override
        public void write(LogResult row, Context context) {
            validate(row);
            buffer.add(row);
        }
        @Override
        public void flush(boolean endOfInput) throws IOException {
            // INSERT INTO flinklab.log_results (...) VALUES ...
            // 非 2xx → IOException
        }
        static void validate(LogResult row) {
            // 枚举白名单 + containsForbidden('"','\'','\\')
        }
    }
}
```

**Apply:** 表 `flinklab.reco_results`；列含 `feature_source` 白名单 `REDIS|STATE_ONLY`；字符串消毒同 T-04-01。

---

### `scripts/verify.sh` (test, request-response)

**Analog:** `projects/p01-log-ai-platform/scripts/verify.sh`

**CH 权威 + Kafka 仅诊断** (lines 1-48, 71-77):

```bash
# ClickHouse 为唯一放行条件；Kafka 不得单独放行
CH_MATCH_QUERY="SELECT count() FROM flinklab.log_results WHERE rule_label='${RULE_LABEL}'"
diag_kafka() {
  # kafka-console-consumer ... 仅排障
  echo "diag: Kafka ...（仅排障，不作为放行条件）" >&2
}
# MATCH_COUNT < MIN_COUNT → FAIL + diag_kafka
```

**Apply:** `flinklab.reco_results`；可选 `FEATURE_SOURCE` 白名单拼 SQL；**禁止** `redis-cli KEYS` 放行。

---

### `scripts/drill_redis_degrade.sh` (test, event-driven)

**Analog:** `projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh`

**结构** (lines 1-117 骨架):

```bash
# 1) require Flink REST + ClickHouse
# 2) 提交作业（p01: ai off；p02: 正常提交后 stop redis）
# 3) truncate CH → gen scenario → 轮询 verify.sh
# 4) 超时 FAIL
```

**p02 差异:**

| 步骤 | p01 | p02 |
|------|-----|-----|
| 外依赖失效 | `--ai.enabled=false` + 不可达 endpoint | `docker compose stop redis`（或 `stop fll-redis`） |
| 断言 | `RULE_LABEL=AUTH_FAIL` | CH 有行且可选 `feature_source=STATE_ONLY` |
| 恢复 | 不要求 Ollama | 演练结束可 `compose start redis`（可选） |

---

### `scripts/gen_reco_events.py` (utility, pub-sub)

**Analog:** `projects/p01-log-ai-platform/scripts/gen_log_events.py`

**uv script + scenario + rate** (lines 1-65):

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
SCENARIOS = ("rule-auth-fail", ...)
MAX_RATE = 5000
MAX_DURATION = 600
# --bootstrap localhost:9094 --topic logs.events --scenario | --rate/--duration
```

**Apply:** topic `reco.events`；scenario 如 `feature-topk`（可判定特征 + Top-K）；事件字段 `userId/itemId/eventType/eventTime`；eventType 大写枚举。

---

### `scripts/loadtest.sh` (utility, streaming)

**Analog:** `projects/p01-log-ai-platform/scripts/loadtest.sh`

**Defaults** (lines 14-20): `RATE=100`、`WARMUP_SEC=30`、`DURATION_SEC=90`；刮 Prom + CH → 写 `docs/baseline.md`；依赖不可达 exit 1。

---

### `sql/clickhouse_reco_results.sql` (migration, file-I/O)

**Analog:** `projects/p01-log-ai-platform/sql/clickhouse_log_results.sql`

```sql
-- 单条 CREATE；CH HTTP 禁多语句
CREATE TABLE IF NOT EXISTS flinklab.log_results ( ... )
ENGINE = MergeTree
ORDER BY (event_time, service, trace_id)
```

**Apply:** `flinklab.reco_results`；ORDER BY 建议 `(ts, user_id, item_id)`；含 `feature_source`。

---

### `sql/postgres_reco_items.sql` (migration, file-I/O)

**Analog:** 无完整项目级样板（**partial**）。  
**Hints:** e07-C2 JDBC 表在 postgres； Discretion 列 `item_id PK, category, title, base_weight` + 50–100 INSERT。  
**Seed 执行:** `make up-p02` 内 `docker compose exec -T postgres psql … -f`（kafka 镜像无 psql）。

---

### `docker/docker-compose.yml` → `p02-init` (config, event-driven)

**Analog:** `p01-init` (lines 346-380)

```yaml
p01-init:
  image: ${KAFKA_IMAGE}
  profiles: ["p01"]
  restart: "no"
  volumes:
    - ../projects/p01-log-ai-platform/sql/clickhouse_log_results.sql:/ddl/...:ro
  depends_on:
    kafka: { condition: service_healthy }
    clickhouse: { condition: service_healthy }
  command:
    - |
      kafka-topics.sh --create --if-not-exists --topic logs.events ...
      wget -qO- --post-file=/ddl/... "http://clickhouse:8123/?user=..."
```

**Apply:** topics `reco.events` + `reco.results`；挂载 `clickhouse_reco_results.sql`；**禁止**绑进 default `make up`。

---

### `docker/Makefile` → `up-p02` (config, request-response)

**Analog:** `docker/Makefile` lines 19-21

```makefile
up-p01:
	docker compose --profile p01 up -d p01-init
```

**Apply:** 镜像 `up-p02`；可在目标内追加 postgres seed。

---

### Docs 包（ARCHITECTURE / RESUME / baseline / ADR）

| File | Analog | Copy |
|------|--------|------|
| `docs/adr/0001-dual-channel-features.md` | p01 `docs/adr/0001-ai-path-degradable.md` | Status/Date/Context/Decision/Consequences；主题改为 State+Redis vs 纯状态/纯外存 |
| `docs/ARCHITECTURE.md` | p01 `docs/ARCHITECTURE.md` | 边界 + 总图 ASCII + 算子顺序 + 两条演练入口 |
| `docs/RESUME.md` | p01/p03 `docs/RESUME.md` | 简历条目形态 |
| `docs/baseline.md` | p01 `docs/baseline.md` | OrbStack 实测数字槽位 |
| `README.md`（项目） | p01 README | 八段式 |
| `docs/README.md` 15-02 | 同文件 15-01 行 | 完成态链接回填 |

**ADR 形态** (p01 ADR lines 1-34):

```markdown
# ADR-0001：AI 路径可降级——...
- **Status:** Accepted
- **Date:** 2026-07-18
## Context
## Decision
## Consequences
```

---

### Tests

| New test | Analog | Pattern |
|----------|--------|---------|
| `ParseBehaviorJsonTest` | `ParseLogJsonTest` | `tryParse` 合法/缺字段/非法字符 |
| `RuleScorerTest` | `RuleTaggerTest` | 纯静态打分断言权重与 Top-K |
| `SessionFeatureFunctionTest` | 无强 MiniCluster 样板 | 测快照累积纯逻辑 / 辅助函数；避免强依赖 MiniCluster |

**Wave 0 RED** (ParseLogJsonTest lines 12-16): 先写测试引用未实现类建立反馈环。

---

### `README.md`（根）jedis SSOT 行

**Analog:** `examples/pom.xml` `<jedis.version>5.2.0</jedis.version>`  
**Apply:** 版本矩阵补行 `jedis | 5.2.0 | Redis Pipeline`；**先于** p02 pom 合入。

## Shared Patterns

### 算子 `.uid(...)` 命名
**Source:** p01 `LogAiJob` / p03 `VehicleAlertJob`  
**Apply to:** 所有 Source/Process/Map/Sink  
**Form:** `p02-source-reco-events`、`p02-parse-behavior`、`p02-session-feature`、`p02-redis-feature`、`p02-rule-scorer`、`p02-sink-kafka-results`、`p02-sink-clickhouse-results`

### ClickHouse = 唯一验收权威
**Source:** `projects/p01-log-ai-platform/scripts/verify.sh`  
**Apply to:** `verify.sh`、`drill_redis_degrade.sh`、`match` Makefile 目标  
```bash
# Kafka / redis-cli KEYS 仅 diag_*，不得单独 exit 0
```

### 外依赖降级不打挂作业
**Source:** p01 `OllamaRiskAsyncFunction.timeout` + `drill_ai_degrade.sh`  
**Apply to:** `RedisFeatureWriter` 写失败、`RuleScorer` Redis 读失败  
```java
// catch → metric.inc() → 继续 emit，feature_source=STATE_ONLY
```

### JSON 解析丢弃脏数据
**Source:** `ParseLogJson.tryParse`  
**Apply to:** `ParseBehaviorJson`  
```java
// Optional.empty()；拒引号/反斜杠；枚举白名单
```

### SinkV2 字段消毒
**Source:** `ClickHouseLogSink.BatchWriter.validate`  
**Apply to:** `ClickHouseRecoSink`  
```java
// 表名列名常量；枚举白名单；拒 '"','\'','\\'
```

### Compose profile 隔离
**Source:** `docker/docker-compose.yml` p01-init + `docker/Makefile` up-p01  
**Apply to:** p02-init / up-p02  
```yaml
profiles: ["p02"]  # 禁止进入 default make up
```

### Redis 生产写必须 CheckpointedFunction
**Source:** e07-C7 `RedisBatchWriter`  
**Apply to:** `RedisFeatureWriter`  
```java
// 禁止只抄 e12-06 省略 Operator State 的教学简化版
```

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `.../model/ItemCatalog.java` | model | CRUD | 仓库无独立 catalog POJO；字段按 Discretion 新建即可 |
| `sql/postgres_reco_items.sql` | migration | file-I/O | 无项目级 PG seed SQL 样板；仅有 e07 JDBC URL/表名提示，需新建 CREATE+INSERT |

## Metadata

**Analog search scope:**  
`projects/p01-log-ai-platform/`、`projects/p03-vehicle-monitoring/`、`examples/e12-06-streaming-feature/`、`examples/e07-connectors/`、`examples/e11-async-io/`、`docker/`、`docs/README.md`、`examples/pom.xml`

**Files scanned:** ~45（含脚本/SQL/文档样板）  
**Pattern extraction date:** 2026-07-18  
**Primary skeleton:** p01（最近完成）  
**Primary domain features:** e12-06 MapState + e07-C7 Checkpointed Redis  
**Primary dual-sink:** p03 VehicleAlertJob  
**Primary degrade drill:** p01 drill_ai_degrade → 换 Redis stop
