# Phase 4: p01 日志 AI 平台 - Pattern Map

**Mapped:** 2026-07-18
**Files analyzed:** 28
**Analogs found:** 27 / 28

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `projects/p01-log-ai-platform/pom.xml` | config | batch | `projects/p03-vehicle-monitoring/pom.xml` | exact |
| `projects/p01-log-ai-platform/Makefile` | config | request-response | `projects/p03-vehicle-monitoring/Makefile` | exact |
| `projects/p01-log-ai-platform/README.md` | config | file-I/O | `projects/p03-vehicle-monitoring/README.md` | role-match |
| `projects/p01-log-ai-platform/sql/clickhouse_log_results.sql` | migration | file-I/O | `projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql` | exact |
| `projects/p01-log-ai-platform/src/.../LogAiJob.java` | controller | streaming | `projects/p03-vehicle-monitoring/.../VehicleAlertJob.java` | exact |
| `projects/p01-log-ai-platform/src/.../JobConfig.java` | config | request-response | `projects/p03-vehicle-monitoring/.../JobConfig.java` | exact |
| `projects/p01-log-ai-platform/src/.../model/LogEvent.java` | model | transform | `projects/p03-vehicle-monitoring/.../model/VehicleEvent.java` | exact |
| `projects/p01-log-ai-platform/src/.../model/LogResult.java` | model | transform | `projects/p03-vehicle-monitoring/.../model/AlertEvent.java` | exact |
| `projects/p01-log-ai-platform/src/.../ParseLogJson.java` | middleware | transform | `projects/p03-vehicle-monitoring/.../ParseVehicleJson.java` | exact |
| `projects/p01-log-ai-platform/src/.../enrich/FeatureEnricher.java` | service | streaming | `examples/e12-06-streaming-feature/.../StreamingFeatureJob.java` (`SessionCategoryFeature`) | role-match |
| `projects/p01-log-ai-platform/src/.../rule/RuleTagger.java` | service | transform | `examples/e12-15-observability/.../ObservableAgentJob.java` (`ObservableThresholdCheck`) | partial |
| `projects/p01-log-ai-platform/src/.../ai/OllamaRiskAsyncFunction.java` | service | event-driven | `examples/e11-async-io/.../C2TimeoutRetryJob.java` (`FlakyEnrich`) | exact |
| `projects/p01-log-ai-platform/src/.../guardrail/GuardrailFunction.java` | middleware | event-driven | `examples/e12-17-streaming-guardrail/.../StreamingGuardrailJob.java` | exact |
| `projects/p01-log-ai-platform/src/.../cost/BudgetGateFunction.java` | middleware | streaming | `examples/e12-18-streaming-cost-control/.../StreamingCostControlJob.java` | exact |
| `projects/p01-log-ai-platform/src/.../sink/ClickHouseLogSink.java` | service | streaming | `projects/p03-vehicle-monitoring/.../sink/ClickHouseAlertSink.java` | exact |
| `projects/p01-log-ai-platform/src/test/.../ParseLogJsonTest.java` | test | batch | `projects/p03-vehicle-monitoring/.../PatternActivationGateTest.java` | role-match |
| `projects/p01-log-ai-platform/src/test/.../RuleTaggerTest.java` | test | batch | `projects/p03-vehicle-monitoring/.../PatternActivationGateTest.java` | role-match |
| `projects/p01-log-ai-platform/src/test/.../BudgetGateTest.java` | test | batch | `projects/p03-vehicle-monitoring/.../EventCountAggTest.java` | role-match |
| `projects/p01-log-ai-platform/scripts/gen_log_events.py` | utility | pub-sub | `projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py` | exact |
| `projects/p01-log-ai-platform/scripts/verify.sh` | utility | request-response | `projects/p03-vehicle-monitoring/scripts/verify.sh` | exact |
| `projects/p01-log-ai-platform/scripts/verify_ai.sh` | utility | request-response | `projects/p03-vehicle-monitoring/scripts/verify.sh` | role-match |
| `scripts/smoke_p01_profile.sh` | utility | request-response | `docker/Makefile` (`up-p03` compose config 差集) + `projects/p03-vehicle-monitoring/scripts/verify.sh`（非 0 失败纪律） | role-match |
| `projects/p01-log-ai-platform/scripts/loadtest.sh` | utility | batch | `projects/p03-vehicle-monitoring/scripts/loadtest.sh` | exact |
| `projects/p01-log-ai-platform/scripts/drill_ai_degrade.sh` | utility | request-response | `projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh` | role-match |
| `projects/p01-log-ai-platform/docs/ARCHITECTURE.md` | config | file-I/O | `projects/p03-vehicle-monitoring/docs/ARCHITECTURE.md` | exact |
| `projects/p01-log-ai-platform/docs/RESUME.md` | config | file-I/O | `projects/p03-vehicle-monitoring/docs/RESUME.md` | exact |
| `projects/p01-log-ai-platform/docs/baseline.md` | config | file-I/O | `projects/p03-vehicle-monitoring/docs/baseline.md` | exact |
| `projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md` | config | file-I/O | `projects/p03-vehicle-monitoring/docs/adr/0001-cep-broadcast-precompiled.md` | exact |
| `projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md` | config | file-I/O | —（可并入 README §验证；无独立同构文件） | none |
| `docker/docker-compose.yml` (`p01-init`) | config | pub-sub | `docker/docker-compose.yml` (`p03-init`) | exact |
| `docker/Makefile` (`up-p01` / `submit-p01`) | config | request-response | `docker/Makefile` (`up-p03` / `submit-p03`) | exact |
| `docs/README.md`（回填 15-01） | config | file-I/O | `docs/README.md` 现有 15-xx 行 | exact |

## Pattern Assignments

### `projects/p01-log-ai-platform/pom.xml` (config, batch)

**Analog:** `projects/p03-vehicle-monitoring/pom.xml`

**Core pattern**（独立模块 + SSOT 属性 + shade 主类，lines 7–26 / 90–108）:
```xml
<!-- 独立 Maven 模块（不挂 examples 父工程）；版本对齐根 README / examples/pom 属性区 -->
<groupId>com.flywhl.flinklab</groupId>
<artifactId>p03-vehicle-monitoring</artifactId>
<!-- properties: flink.version=2.2.1, flink.kafka.connector.version=5.0.0-2.2, jackson=2.17.2, junit=5.10.2 -->
<!-- shade ManifestResourceTransformer mainClass → 改为 com.flywhl.flinklab.p01.LogAiJob -->
<!-- p01 勿加 flink-cep / Agents / Milvus SDK；可选 flink-metrics-dropwizard（与 e12-15 一致） -->
```

**Copy notes:** 去掉 `flink-cep`；保留 kafka connector + jackson + junit + shade；`local` profile 供本机 exec 可选。

---

### `projects/p01-log-ai-platform/.../JobConfig.java` (config, request-response)

**Analog:** `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/JobConfig.java`

**Core `--key` / `--key=value` 解析**（lines 78–91）:
```java
/** 支持 {@code --key value} 与 {@code --key=value}。 */
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

**p01 扩展字段（RESEARCH Pattern 1，原样叠在同一类上）:**
| Flag | Default |
|------|---------|
| `--ai.enabled` | `false` |
| `--ai.endpoint` | `http://host.docker.internal:11434` |
| `--ai.model` | `qwen3:8b`（可覆写） |
| `--ai.timeout-ms` | `8000` |
| `--ai.capacity` | `16` |
| `--ai.retry` | `2` |
| `--budget.max-ai-calls` | `120` |
| `--guardrail.keywords` | 逗号分隔关键词 |
| `--events-topic` | `logs.events` |
| `--kafka-bootstrap` / `--clickhouse-url` | 同 p03 默认 |

---

### `projects/p01-log-ai-platform/.../LogAiJob.java` (controller, streaming)

**Analog:** `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java`

**Imports / env / Kafka source wiring**（lines 47–81）:
```java
JobConfig cfg = JobConfig.from(args);
Configuration conf = Configuration.fromMap(Map.of(
        "state.backend.type", cfg.stateBackendType,
        "execution.checkpointing.incremental", "true"));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(cfg.checkpointTimeoutMs);

KafkaSource<String> eventSource = KafkaSource.<String>builder()
        .setBootstrapServers(cfg.kafkaBootstrap)
        .setTopics(cfg.eventsTopic)
        .setGroupId(cfg.groupId)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<VehicleEvent> events = env
        .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-vehicle-events")
        .uid("p03-source-vehicle-events")
        .flatMap(new ParseVehicleJson())
        .name("parse-vehicle-json")
        .uid("p03-parse-vehicle-json")
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTime)
                        .withIdleness(Duration.ofSeconds(30)));
```

**Sink 双写纪律**（lines 142–151；p01 CH 权威，Kafka `logs.alerts` 可选诊断）:
```java
gated
        .sinkTo(new ClickHouseAlertSink(
                cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
        .name("clickhouse-vehicle-alerts")
        .uid("p03-sink-clickhouse-alerts");
```

**p01 差异接线:**
- `ai.enabled=false`：Parse → FeatureEnricher → RuleTagger → BudgetGate（透传）→ Guardrail → CH（`ai_source=DISABLED`）
- `ai.enabled=true`：Parse → FeatureEnricher → RuleTagger → **BudgetGate（超限短路，不进 Async）** →（allow 时）`AsyncDataStream.unorderedWaitWithRetry` → Guardrail → CH
- 所有算子强制 `.uid("p01-...")`

---

### `projects/p01-log-ai-platform/.../ParseLogJson.java` (middleware, transform)

**Analog:** `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/ParseVehicleJson.java`

**Imports + 脏数据丢弃**（lines 1–66）:
```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
// ...
public final class ParseVehicleJson extends RichFlatMapFunction<String, VehicleEvent> {
    private transient ObjectMapper mapper;
    @Override
    public void open(OpenContext openContext) { mapper = new ObjectMapper(); }
    @Override
    public void flatMap(String json, Collector<VehicleEvent> out) {
        if (json == null || json.isBlank()) { return; }
        try {
            JsonNode node = mapper.readTree(json);
            // 必填字段缺失 / 白名单失败 / 非法字符 → return（不抛、不拖垮作业）
            // ...
        } catch (Exception e) {
            LOG.warn("解析 VehicleEvent JSON 失败，丢弃: {}", e.toString());
        }
    }
}
```

**p01 映射:** 必填 `service`/`level`/`message`/`traceId`/`eventTime`；`level` 白名单如 `ERROR|WARN|INFO|DEBUG`；`service`/`traceId` 拒绝引号与反斜杠（对齐 Sink 校验）。

---

### `projects/p01-log-ai-platform/.../model/LogEvent.java` + `LogResult.java` (model, transform)

**Analog:** `VehicleEvent.java` / `AlertEvent.java`

**POJO 约定**（VehicleEvent lines 8–23）:
```java
public final class VehicleEvent {
    public String vin;
    public String signalType;
    public double value;
    public long eventTime;
    public VehicleEvent() { }
    public VehicleEvent(String vin, String signalType, double value, long eventTime) { /* ... */ }
}
```

**p01:** `LogEvent` 公开字段 + 无参构造（Flink POJO）；`LogResult` 增加 `ruleLabel` / `aiRisk` / `aiSource` / `featureJson`（对齐 RESEARCH DDL）。

---

### `projects/p01-log-ai-platform/.../enrich/FeatureEnricher.java` (service, streaming)

**Analog:** `examples/e12-06-streaming-feature/.../StreamingFeatureJob.java` — `SessionCategoryFeature`（MapState；**勿**抄 Redis 写路径）

**Keyed State 特征**（lines 73–90）:
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

**p01 映射:** `keyBy(service 或 service+traceId)`；用 MapState/ValueState 累计 ERROR 计数等规则特征 → 写入 `LogResult.featureJson`；**禁止**默认依赖 Redis（CONTEXT/RESEARCH A1）。

---

### `projects/p01-log-ai-platform/.../rule/RuleTagger.java` (service, transform)

**Analog（部分）:** `examples/e12-15-observability/.../ObservableAgentJob.java` — 阈值判定 + 可选 Counter

**阈值判定骨架**（lines 66–81）:
```java
public void processElement(Event e, Context ctx, Collector<String> out) {
    boolean triggered = e.amount > THRESHOLD;
    if (triggered) {
        alertCounter.inc();
        out.collect("ALERT ...");
    }
}
```

**p01 映射:** 纯函数/ProcessFunction：`AUTH_FAIL`（message 含 auth 失败关键词 + level=ERROR）、`ERROR_BURST`（特征计数超阈）、否则 `NONE`；输出 `rule_label` 枚举白名单供 verify。

---

### `projects/p01-log-ai-platform/.../ai/OllamaRiskAsyncFunction.java` (service, event-driven)

**Analog:** `examples/e11-async-io/src/main/java/com/flywhl/flinklab/e11/C2TimeoutRetryJob.java`

**Async + 重试 + timeout 降级**（lines 38–79）:
```java
var retryStrategy = new AsyncRetryStrategies
        .FixedDelayRetryStrategyBuilder<String>(3, 200L)
        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
        .build();

AsyncDataStream.unorderedWaitWithRetry(
                Labs.events(env, "clicks", 30, 5, 10, 500),
                new FlakyEnrich(), 3, TimeUnit.SECONDS, 100, retryStrategy)
        .uid("e11-c2-retry");

public static final class FlakyEnrich extends RichAsyncFunction<Event, String> {
    private transient ExecutorService pool;
    @Override
    public void open(OpenContext ctx) {
        pool = Executors.newFixedThreadPool(8);
    }
    @Override
    public void asyncInvoke(Event e, ResultFuture<String> rf) {
        client.lookup(e.userId, pool).whenComplete((p, err) -> {
            if (err != null) {
                rf.completeExceptionally(err);   // 交给重试策略
            } else {
                rf.complete(Collections.singleton("OK ..."));
            }
        });
    }
    @Override
    public void timeout(Event e, ResultFuture<String> rf) {
        // 降级而非失败 —— 链路可用性与模型可用性解耦
        rf.complete(Collections.singleton("DEGRADED ..."));
    }
    @Override
    public void close() {
        if (pool != null) { pool.shutdown(); }
    }
}
```

**线程池异步纪律（FakeDimClient lines 24–39）:** 外呼必须经 `Executor` / `HttpClient.sendAsync`；禁止在 `asyncInvoke` 同步阻塞 `get()`。

**p01 映射:**
- 泛型改为 `LogResult`；timeout 填 `ai_risk=UNKNOWN`/`ai_source=DEGRADED`，保留 `rule_label`
- HTTP：`POST {endpoint}/api/chat`，body `stream:false` + `format:json` + `think:false`（RESEARCH）
- Counter：`ai_calls` / `ai_timeouts` / `ai_degrades`（见 Shared Patterns · Metrics）
- capacity≤16；仅 `cfg.aiEnabled` 分支挂接

---

### `projects/p01-log-ai-platform/.../guardrail/GuardrailFunction.java` (middleware, event-driven)

**Analog:** `examples/e12-17-streaming-guardrail/.../StreamingGuardrailJob.java`

**关键词 BLOCK**（lines 86–111）:
```java
public static final class GuardrailProcessFn
        extends KeyedBroadcastProcessFunction<String, String, GuardrailRule, String> {
    @Override
    public void processElement(String output, ReadOnlyContext ctx, Collector<String> out) {
        ReadOnlyBroadcastState<String, GuardrailRule> rules = ctx.getBroadcastState(RULES_DESC);
        for (var entry : rules.immutableEntries()) {
            if (output.contains(entry.getValue().keyword)) {
                out.collect("BLOCK  [规则%s命中\"%s\"] %s"
                        .formatted(entry.getValue().ruleId, entry.getValue().keyword, output));
                return;
            }
        }
        out.collect("PASS   " + output);
    }
}
```

**p01 MVP（RESEARCH Open Q2）:** 优先 **静态** `JobConfig.guardrailKeywords` 的 `ProcessFunction`（单测易）；Broadcast 热更新为可选加分。命中 → `ai_source=BLOCKED` + `guardrail_blocks` Counter；仍落 CH。

---

### `projects/p01-log-ai-platform/.../cost/BudgetGateFunction.java` (middleware, streaming)

**Analog:** `examples/e12-18-streaming-cost-control/.../StreamingCostControlJob.java`

**预算熔断判断**（lines 93–106）:
```java
public static final class BudgetEnforcer
        extends ProcessWindowFunction<Double, String, String, TimeWindow> {
    @Override
    public void process(String tenant, Context ctx, Iterable<Double> totals, Collector<String> out) {
        double total = totals.iterator().next();
        if (total > BUDGET_PER_MINUTE_USD) {
            out.collect("BUDGET-EXCEEDED  tenant=%s ... → 建议:切换降级模型/限流"
                    .formatted(tenant, total, BUDGET_PER_MINUTE_USD));
        } else {
            out.collect("OK  tenant=%s ...");
        }
    }
}
```

**p01 映射:** 用滚动窗口或 Keyed 计数状态实现 `budget.max-ai-calls`；超限后跳过 Async、走规则路径，`budget_trips.inc()`，`ai_source=DEGRADED`（或 `RULE`）。单测测纯计数/熔断逻辑（对齐 Wave 0 `BudgetGateTest`）。

---

### `projects/p01-log-ai-platform/.../sink/ClickHouseLogSink.java` (service, streaming)

**Analog:** `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseAlertSink.java`

**SinkV2 批量 HTTP INSERT + 白名单校验**（lines 23–127）:
```java
public final class ClickHouseAlertSink implements Sink<AlertEvent> {
    // createWriter → BatchWriter：buffer → flush 拼 VALUES → HttpURLConnection POST
    // 非 2xx → IOException
    static void validate(AlertEvent alert) {
        if (!ALLOWED_ALERT_TYPES.contains(alert.alertType)) { throw ...; }
        if (containsForbidden(alert.vin) /* ' " \ */) { throw ...; }
    }
}
```

**p01 映射:** 表 `flinklab.log_results`；白名单 `rule_label` / `ai_risk` / `ai_source`；`message` 单引号替换为空格；**禁止**新 JDBC 驱动。

---

### `sql/clickhouse_log_results.sql` (migration, file-I/O)

**Analog:** `projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql`

```sql
-- ClickHouse HTTP 禁多语句；单文件单 CREATE，供 p01-init 一次 POST
CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts ( ... )
ENGINE = MergeTree
ORDER BY (event_time, vin)
```

**p01:** 使用 RESEARCH 建议 DDL（`service/level/message/trace_id/event_time/feature_json/rule_label/ai_risk/ai_source/ingest_time`）。

---

### `scripts/verify.sh` + `verify_ai.sh` (utility, request-response)

**Analog:** `projects/p03-vehicle-monitoring/scripts/verify.sh`

**CH 权威 + SQL 白名单 + Kafka 仅诊断**（lines 1–80）:
```bash
set -euo pipefail
# case 白名单枚举后再拼 SQL —— 禁止任意环境变量直拼
case "${PATTERN_ID}" in
  HARSH_THEN_FAULT|TRIPLE_HARSH|DTC_PAIR) ;;
  *) echo "FAIL: PATTERN_ID 非法..." >&2; exit 1 ;;
esac
CH_MATCH_QUERY="SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH' AND pattern_id='${PATTERN_ID}'"
# docker compose exec clickhouse clickhouse-client --query ...
# MATCH_COUNT < MIN_COUNT → FAIL；diag_kafka 永不单独 exit 0
```

**p01:**
- `verify.sh`：`rule_label='AUTH_FAIL'`（白名单）count ≥ 1；**不**查 Ollama
- `verify_ai.sh`：前置 `curl` Ollama tags；断言 `ai_source='AI' AND ai_risk IN ('HIGH','MEDIUM','LOW')`

---

### `scripts/gen_log_events.py` (utility, pub-sub)

**Analog:** `projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py`

**uv script + scenario + rate/duration**（lines 1–30 / 58–85）:
```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
# --bootstrap default localhost:9094（宿主机）；作业内 kafka:9092
# --scenario 离散剧本；--rate/--duration 压测；上限防误打
```

**p01 scenarios:** `rule-auth-fail` / `rule-error-burst` / `ai-risk-high`；JSON 字段 `service,level,message,traceId,eventTime`。

---

### `scripts/loadtest.sh` (utility, batch)

**Analog:** `projects/p03-vehicle-monitoring/scripts/loadtest.sh`

**Prom 刮取 + 写 baseline.md**（lines 1–20 / 22–45）:
```bash
# gen --rate/--duration → Prometheus query → docs/baseline.md
# 禁止 k6/JMeter；依赖不可达 exit 1
PROM_URL="${PROM_URL:-http://localhost:9090}"
RATE="${RATE:-100}"
DURATION_SEC="${DURATION_SEC:-120}"
prom_query() { curl -sfG "${PROM_URL}/api/v1/query" --data-urlencode "query=${q}"; }
```

---

### `scripts/drill_ai_degrade.sh` (utility, request-response)

**Analog:** `projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh`（演练脚本结构，非 watermark 语义）

**结构纪律**（lines 1–15）:
```bash
# 两阶段 / 显式断言 / 失败非 0；可选附录不挡 exit 0
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"
```

**p01 语义:** AI off 或 endpoint 不可达 → 造 `rule-auth-fail` → `verify.sh` 仍绿（LOG-02 硬演练）。

---

### `docker/docker-compose.yml` `p01-init` + `docker/Makefile` (config)

**Analog:** `p03-init`（compose lines 296–343）+ `up-p03` / `submit-p03`

```yaml
p03-init:
  image: ${KAFKA_IMAGE}
  profiles: ["p03"]
  restart: "no"
  volumes:
    - ../projects/p03-vehicle-monitoring/sql/...:/ddl/...:ro
  # kafka-topics --create --if-not-exists
  # wget --post-file 单语句 DDL（禁多语句）
```

```makefile
up-p03:
	docker compose --profile p03 up -d p03-init
# 禁止改 default `up` 目标语义
```

**p01:** `profiles: ["p01"]`；topics `logs.events`（+ 可选 `logs.alerts`）；挂载 `clickhouse_log_results.sql`；`make up-p01` / `submit-p01` 显式 `p01-init`。

---

### `Makefile`（项目内）+ 文档包

**Analog:** `projects/p03-vehicle-monitoring/Makefile` + `docs/adr/0001-*.md` + ARCHITECTURE/RESUME/baseline

```makefile
.PHONY: package verify test submit gen loadtest drill-watermark
package: ; mvn -q clean package
verify:  ; bash scripts/verify.sh
submit: package
	# cp jar → docker/jobs；flink run -c 固定主类
loadtest: ; bash scripts/loadtest.sh
```

**p01 目标:** `verify` / `verify-ai` / `loadtest` / `drill-degrade`；ADR 主题锁定「AI 路径可降级：主构建零硬依赖 Preview/外部模型」（D-15）。

---

### 单测（Parse / Rule / Budget）

**Analog:** `PatternActivationGateTest.java`（轻量、无 MiniCluster）

```java
class PatternActivationGateTest {
    @Test
    void emptyStateDefaultsToHarshThenFaultOnly() {
        Set<String> active = PatternActivationGate.resolveActivePatterns(null);
        assertEquals(Set.of(PatternIds.HARSH_THEN_FAULT), active);
    }
}
```

**p01:** 测纯函数/包内可见辅助方法；`BudgetGateTest` 测计数熔断；不强制 Testcontainers。

---

## Shared Patterns

### Job 参数解析（禁止 ParameterTool）
**Source:** `projects/p03-vehicle-monitoring/.../JobConfig.java` lines 78–91  
**Apply to:** `JobConfig`、submit 文档、verify-ai 传参

### 算子 `.uid(...)` + Checkpoint
**Source:** `VehicleAlertJob.java` lines 54–56, 73–77  
**Apply to:** 全部 DataStream 算子（source/parse/enrich/ai/guardrail/sink）

### Async I/O 超时降级（禁止同步堵 mailbox）
**Source:** `e11 C2TimeoutRetryJob` + `FakeDimClient`  
**Apply to:** `OllamaRiskAsyncFunction`；timeout 完成降级记录，不抛异常打挂作业

### ClickHouse 权威验收 + verify SQL 白名单
**Source:** `scripts/verify.sh` + `ClickHouseAlertSink.validate`  
**Apply to:** `verify.sh` / `verify_ai.sh` / Sink / DDL init

### 自定义 Metrics（LOG-04）
**Source:** `examples/e12-15-observability/.../ObservableAgentJob.java` lines 54–63  
**Apply to:** AI / Guardrail / Budget 算子 `open()`:
```java
alertCounter = getRuntimeContext().getMetricGroup()
        .counter("alerts_triggered");
// p01: MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("p01");
// ai_calls / ai_timeouts / ai_degrades / guardrail_blocks / budget_trips
```

### Compose profile 隔离
**Source:** `docker/docker-compose.yml` `p03-init` + `docker/Makefile` `up-p03`  
**Apply to:** `p01-init`；Milvus 继续 `--profile ai`，不绑进 `p01` / default `up`

### 文档八段式 + ADR 形态
**Source:** p03 `README.md` / `docs/adr/0001-cep-broadcast-precompiled.md`  
**Apply to:** p01 README、ADR-0001 可降级 AI、ARCHITECTURE、RESUME；回填 `docs/README.md` 15-01

### 造数 / 压测（无 k6）
**Source:** `gen_vehicle_events.py` + `loadtest.sh`  
**Apply to:** `gen_log_events.py` + `loadtest.sh` → `docs/baseline.md`

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `docs/DEGRADE-CHECKLIST.md` | config | file-I/O | 仓库无独立降级勾选表文件；可并入 README §验证（D-05），规划时按 CONTEXT 勾选四格自拟结构即可 |

## Metadata

**Analog search scope:** `projects/p03-vehicle-monitoring/`, `examples/e11-async-io/`, `examples/e12-06-streaming-feature/`, `examples/e12-15-observability/`, `examples/e12-17-streaming-guardrail/`, `examples/e12-18-streaming-cost-control/`, `docker/`, `docs/README.md`  
**Files scanned:** ~55（p03 全树 + e11/e12-15/17/18/06 主类 + docker compose/Makefile）  
**Strong analogs capped at:** 5 主簇（p03 工程纪律、e11 Async、e12-17 护栏、e12-18 预算、e12-15/e12-06 指标与状态）  
**Pattern extraction date:** 2026-07-18
