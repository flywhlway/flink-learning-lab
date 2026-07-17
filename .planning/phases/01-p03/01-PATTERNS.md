# Phase 1: p03 告警链路样板 - Pattern Map

**Mapped:** 2026-07-18
**Files analyzed:** 19
**Analogs found:** 17 / 19

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `docker/docker-compose.yml`（增 `p03-init`） | config | batch | 同文件 `milvus-*` `profiles: ["ai"]` | exact |
| `docker/Makefile`（增 `up-p03` / 可选 `submit-p03`） | config | request-response | `docker/Makefile` `up-ai` / `submit-e01` | exact |
| `projects/p03-vehicle-monitoring/pom.xml` | config | batch | `templates/job-datastream/pom.xml` | exact |
| `projects/p03-vehicle-monitoring/src/.../JobConfig.java` | config | request-response | `templates/job-datastream/.../JobConfig.java` | exact |
| `projects/p03-vehicle-monitoring/src/.../VehicleAlertJob.java` | service | streaming | `templates/.../JobTemplate.java` + `e01/.../KafkaClickstreamWindowJob.java` | role-match |
| `projects/p03-vehicle-monitoring/src/.../model/VehicleEvent.java` | model | transform | `examples/e01-hello-flink/.../ClickEvent.java` | exact |
| `projects/p03-vehicle-monitoring/src/.../model/AlertEvent.java` | model | transform | `examples/e01-hello-flink/.../ClickEvent.java` | role-match |
| `projects/p03-vehicle-monitoring/src/.../cep/HarshThenFaultPattern.java` | service | event-driven | `examples/e10-cep/.../C5VehicleDtcPatternJob.java` | exact |
| `projects/p03-vehicle-monitoring/src/.../cep/AlertPatternHandler.java` | service | event-driven | `examples/e10-cep/.../C3TimeoutSideOutputJob.java` `CartHandler` | exact |
| `projects/p03-vehicle-monitoring/src/.../sink/ClickHouseAlertSink.java` | service | streaming | `examples/e07-connectors/.../C6ClickHouseHttpSinkJob.java` | exact |
| `projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql` | migration | file-I/O | `docker/config/clickhouse/init/01-init.sql` | exact |
| `projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py` | utility | pub-sub | `scripts/gen_events.py` | exact |
| `projects/p03-vehicle-monitoring/scripts/verify.sh` | test | request-response | `scripts/qa_check.sh` + `docker/init.sh` CH 查询 | partial |
| `projects/p03-vehicle-monitoring/scripts/smoke_profile.sh` | test | request-response | `scripts/qa_check.sh`（compose config 门禁）+ `docker compose config` | role-match |
| `projects/p03-vehicle-monitoring/Makefile` | config | request-response | `docker/Makefile` | role-match |
| `projects/p03-vehicle-monitoring/README.md` | config | file-I/O | `examples/e10-cep/README.md` + `templates/job-datastream/README.md` | role-match |
| `projects/.../HarshThenFaultPatternTest.java`（可选 Wave 0） | test | event-driven | `templates/job-datastream/pom.xml` test deps（无现成测试类） | none |
| `docs/README.md`（模块 15 占位确认） | config | file-I/O | 同文件模块 15 已登记 | exact |
| `CHANGELOG.md` / `PHASES.md`（会话收尾） | config | file-I/O | 根 `CHANGELOG.md` / `PHASES.md` | exact |

## Pattern Assignments

### `docker/docker-compose.yml` — p03-init（config, batch）

**Analog:** 同文件 `milvus-etcd` / `milvus-standalone`（`profiles: ["ai"]`）

**Core profile isolation pattern** (lines 211–219):
```yaml
  # ─────────────────────────── Phase 3 · AI 向量库(profile=ai,按需启动)───────────────────────────
  # 用法:docker compose --profile ai up -d milvus-standalone
  milvus-etcd:
    image: ${MILVUS_ETCD_IMAGE}
    container_name: fll-milvus-etcd
    profiles: ["ai"]
```

**Apply to p03:**
- 新增 `p03-init`（或等价 one-shot）服务，**仅**标注 `profiles: ["p03"]`
- default `docker compose up` / `make up` **不得**启动该服务
- 幂等创建 `vehicle.events` / `vehicle.alerts` + 执行 `clickhouse_alerts.sql`
- 推荐：`restart: "no"` + 挂载项目 SQL / 内联 entrypoint（ai profile 是长期服务；p03-init 是一次性，形态可不同，但 **profiles 隔离纪律同构**）

---

### `docker/Makefile` — up-p03 / submit-p03（config, request-response）

**Analog:** `docker/Makefile`

**Profile 启动 pattern** (lines 7–13):
```makefile
up:            ## 启动全部核心服务(TaskManager 默认 2 个,不含 AI profile)
	docker compose up -d --scale taskmanager=$(TM)

up-ai:         ## 额外启动 Phase 3 AI profile(Milvus 向量库;核心服务需已 up)
	docker compose --profile ai up -d milvus-etcd milvus-minio milvus-standalone
```

**作业提交 pattern** (lines 49–55):
```makefile
submit-e01:    ## 提交 e01 的 Kafka 端到端作业(先在 examples/ 下 mvn package)
	cp ../examples/e01-hello-flink/target/e01-hello-flink-*.jar ./jobs/ 2>/dev/null || \
		(echo "未找到 jar,请先: cd ../examples && mvn -q clean package" && exit 1)
	docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.e01.KafkaClickstreamWindowJob \
		/opt/flink/usrlib/$$(ls ../examples/e01-hello-flink/target | grep '^e01-hello-flink.*\.jar$$' | head -1)
```

**Apply to p03:**
- `up-p03:` → `docker compose --profile p03 up -d ...`（不改动 `up` 目标）
- 可选薄封装 `submit-p03:`：复制 `projects/p03-vehicle-monitoring/target/*.jar` → `docker/jobs` 再 `flink run -c ...VehicleAlertJob`
- 主入口也可放在项目 `Makefile` 的 `submit`/`verify`，docker Makefile 只做薄包装

---

### `projects/p03-vehicle-monitoring/pom.xml`（config, batch）

**Analog:** `templates/job-datastream/pom.xml`

**独立模块 + SSOT 版本** (lines 7–23, 25–53):
```xml
  <!-- 独立脚手架:不挂在 examples 父 pom 下 -->
  <groupId>com.example.flinkjob</groupId>
  <artifactId>job-datastream-template</artifactId>
  <version>0.1.0</version>
  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <flink.version>2.2.1</flink.version>
    <flink.kafka.connector.version>5.0.0-2.2</flink.kafka.connector.version>
    <junit.version>5.10.2</junit.version>
  </properties>
```

**CEP 依赖额外来源:** `examples/e10-cep/pom.xml` line 17:
```xml
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-cep</artifactId></dependency>
```
（模板未含 cep；p03 pom 须显式加 `flink-cep:${flink.version}`，版本与根 README / `examples/pom.xml` 一致。）

**Jackson:** `examples/pom.xml` 管理 `jackson.version=2.17.2`；独立 pom 须自行声明 `jackson-databind`（对齐 e01 ParseJson）。

**Shade + mainClass + local profile:** 直接复制模板 `maven-shade-plugin` 与 `-Plocal` 依赖解 provided 的写法。

---

### `JobConfig.java`（config, request-response）

**Analog:** `templates/job-datastream/.../JobConfig.java`

**参数集中解析** (lines 12–34):
```java
public final class JobConfig {
    public final String jobName;
    public final long checkpointIntervalMs;
    public final String kafkaBootstrap;
    public final String sourceTopic;
    public final String sinkTopic;
    // ...
    private JobConfig(ParameterTool p) {
        this.kafkaBootstrap = p.get("kafka-bootstrap", "kafka:9092");
        this.sourceTopic = p.get("source-topic", "events.raw");
        this.sinkTopic = p.get("sink-topic", "events.enriched");
    }
    public static JobConfig from(String[] args) {
        return new JobConfig(ParameterTool.fromArgs(args));
    }
}
```

**Apply to p03:** 增 `clickhouse-url`（默认 `http://clickhouse:8123/`）、`alerts-topic=vehicle.alerts`、`events-topic=vehicle.events`；容器内 bootstrap 默认 `kafka:9092`，禁止散落硬编码。

---

### `VehicleAlertJob.java`（service, streaming）

**Analogs:**
1. 骨架：`templates/.../JobTemplate.java`
2. Kafka IO + watermark：`examples/e01-hello-flink/.../KafkaClickstreamWindowJob.java`
3. CEP 装配：`C5VehicleDtcPatternJob` + `C3TimeoutSideOutputJob`

**Checkpoint / uid 纪律** (`JobTemplate.java` lines 26–42):
```java
JobConfig cfg = JobConfig.from(args);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(cfg.checkpointTimeoutMs);
buildPipeline(env, cfg);
env.execute(cfg.jobName);
```

**KafkaSource + 事件时间** (`KafkaClickstreamWindowJob.java` lines 63–82):
```java
KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(bootstrap)
        .setTopics(inTopic)
        .setGroupId("e01-clickstream")
        .setStartingOffsets(OffsetsInitializer.latest())  // p03 verify 友好可改 earliest
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<ClickEvent> clicks = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-clicks")
        .uid("e01-j2-source")
        .map(new ParseJson())
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.ts)
                        .withIdleness(Duration.ofSeconds(30)));
```

**KafkaSink 双写一路** (lines 89–98):
```java
.sinkTo(KafkaSink.<String>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build())
```

**ParseJson** (lines 103–117): `RichMapFunction` + `ObjectMapper` 在 `open()` 初始化；p03 映射到 `VehicleEvent`。

**Pipeline 拼装顺序（planner 照此写）:**
1. KafkaSource → Parse → watermark → `keyBy(vin)`
2. `CEP.pattern(...).process(AlertPatternHandler)`
3. 主流 → Kafka alerts + ClickHouse SinkV2
4. `getSideOutput(timeoutTag)` → 同表/同 topic（`alert_type=TIMEOUT`）或可观测旁路

---

### `model/VehicleEvent.java` / `AlertEvent.java`（model, transform）

**Analog:** `examples/e01-hello-flink/.../ClickEvent.java`

**POJO 形态** (lines 10–24):
```java
public class ClickEvent {
    public String userId;
    public String page;
    public long ts;
    public ClickEvent() { }
    public ClickEvent(String userId, String page, long ts) {
        this.userId = userId;
        this.page = page;
        this.ts = ts;
    }
}
```

**Apply:**
- `VehicleEvent`: `vin`, `signalType`, `value`, `eventTime`（epoch millis）— 对应 e10-C5 的 userId/page/amount/ts 语义
- `AlertEvent`: 至少含 `vin`、`alertType`（MATCH/TIMEOUT）、时间戳、简述；保持 public 字段 + 无参构造，便于 `PojoSerializer` / Jackson

---

### `cep/HarshThenFaultPattern.java`（service, event-driven）

**Analog:** `examples/e10-cep/.../C5VehicleDtcPatternJob.java`

**Pattern 核心** (lines 39–43):
```java
Pattern<Event, ?> pattern = Pattern.<Event>begin("harsh")
        .where(SimpleCondition.of(e -> "/search".equals(e.page) && e.amount > 450))
        .followedBy("fault")
        .where(SimpleCondition.of(e -> "/pay".equals(e.page) && e.amount > 480))
        .within(Duration.ofSeconds(30));
```

**Apply to p03:** 换成真实信号枚举，例如：
```java
Pattern.<VehicleEvent>begin("harsh")
    .where(SimpleCondition.of(e -> "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
    .followedBy("fault")
    .where(SimpleCondition.of(e -> "DTC".equals(e.signalType) && e.value > 480))
    .within(Duration.ofSeconds(30));
```
**红线:** 禁止省略 `within`（docs/10-cep）；本 Phase 固定 `followedBy`，不做 Broadcast。

---

### `cep/AlertPatternHandler.java`（service, event-driven）

**Analog:** `examples/e10-cep/.../C3TimeoutSideOutputJob.java` `CartHandler`

**OutputTag + TimedOutPartialMatchHandler** (lines 38–80):
```java
final OutputTag<String> abandoned = new OutputTag<>("abandoned") { };

SingleOutputStreamOperator<String> converted =
        CEP.pattern(keyed, pattern).process(new CartHandler(abandoned));
converted.print();
converted.getSideOutput(abandoned).print();

public static final class CartHandler
        extends PatternProcessFunction<Event, String>
        implements TimedOutPartialMatchHandler<Event> {
    private final OutputTag<String> abandoned;
    @Override
    public void processMatch(Map<String, List<Event>> m, Context ctx, Collector<String> out) {
        out.collect("CONVERTED user=" + m.get("pay").get(0).userId);
    }
    @Override
    public void processTimedOutMatch(Map<String, List<Event>> m, Context ctx) {
        ctx.output(abandoned, "ABANDONED-CART user=" + m.get("cart").get(0).userId);
    }
}
```

**Apply:** Handler 输出 `AlertEvent`（非 String）；`processTimedOutMatch` 经 `ctx.output(timeoutTag, ...)`；作业侧 `getSideOutput` 必须接入可观察 sink（同 CH 表 `alert_type=TIMEOUT` 推荐）。

---

### `sink/ClickHouseAlertSink.java`（service, streaming）

**Analog:** `examples/e07-connectors/.../C6ClickHouseHttpSinkJob.java`

**SinkV2 攒批 + flush** (lines 50–93):
```java
public static final class ClickHouseHttpSink implements Sink<Event> {
    @Override
    public SinkWriter<Event> createWriter(InitContext ctx) {
        return new BatchWriter();
    }
}
// write() 攒批；flush() 在 checkpoint 前调用
String sql = "INSERT INTO flinklab.raw_events(user_id,page,amount,ts) VALUES " + body;
int code = conn.getResponseCode();
buffer.clear();
System.out.println("flush batch=%d httpStatus=%d".formatted(buffer.size(), code)); // BUG: clear 后 size=0
```

**Apply — 必须修正的坑:**
1. **先记录 `batchSize = buffer.size()`，再 `clear()`，再打日志**（RESEARCH Pitfall 6）
2. HTTP 非 2xx **抛 IOException**（示例仅注释提醒）
3. 表名改为 `flinklab.vehicle_alerts`；字段对齐 DDL；对 `vin` 等字符串做校验/转义，防 SQL 注入
4. Writer 构造函数注入 CH URL / 凭据（来自 JobConfig），勿硬编码生产密钥

---

### `sql/clickhouse_alerts.sql`（migration, file-I/O）

**Analog:** `docker/config/clickhouse/init/01-init.sql`

**DDL 风格** (lines 1–14):
```sql
CREATE DATABASE IF NOT EXISTS flinklab;

CREATE TABLE IF NOT EXISTS flinklab.click_window_agg
(
    window_start DateTime64(3),
    window_end   DateTime64(3),
    page         String,
    clicks       UInt64,
    users        UInt64
)
ENGINE = MergeTree
ORDER BY (window_start, page);
```

**Apply:** `CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts (... alert_type Enum8('MATCH'=1,'TIMEOUT'=2), ... ) ENGINE=MergeTree ORDER BY (...)`；**不要**写进 default `01-init.sql`（保护 `make up`），只由 `profiles: ["p03"]` 执行。

---

### `scripts/gen_vehicle_events.py`（utility, pub-sub）

**Analog:** `scripts/gen_events.py`

**uv script + Producer + 宿主机 bootstrap** (lines 1–51):
```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
# 宿主机默认 bootstrap = localhost:9094(容器内 Flink 用 kafka:9092,勿混用)
producer = Producer({
    "bootstrap.servers": args.bootstrap,
    "linger.ms": 20,
    "batch.num.messages": 10_000,
})
```

**Apply:**
- 复制 shebang / PEP 723 依赖块 / `localhost:9094` 默认
- **不要**复用点击页语义；实现可判定序列：同 vin 上 `HARSH_ACCEL` →（within 30s）`DTC`
- 建议 `--mode inject-match|inject-timeout|random` + `--total`，便于 verify 稳定断言
- JSON 字段对齐 `VehicleEvent`（vin/signalType/value/eventTime）

---

### `scripts/smoke_profile.sh`（test, request-response）

**Analogs:**
1. 失败非 0 / 配置门禁：`scripts/qa_check.sh`（compose config 检查）
2. profile 隔离验证：`docker compose config` / `docker compose --profile p03 config`（RESEARCH 自建夹具；仓库无现成 profile 冒烟脚本）

**Apply:** `set -euo pipefail`；断言无 profile 时 `config --services` 不含 `p03-init`，带 `--profile p03` 时含 `p03-init`；配合 Plan 01-01 VEH-01。

---

### `scripts/verify.sh`（test, request-response）

**Analogs（无现成项目 verify.sh — partial）:**
1. 失败非 0 骨架：`scripts/qa_check.sh`
2. CH / Kafka 调用：`docker/init.sh`

**断言失败退出** (`qa_check.sh` lines 5–10, 56):
```bash
set -uo pipefail
FAIL=0
bad()  { printf 'FAIL  %s\n' "$*"; FAIL=1; }
ok()   { printf 'ok    %s\n' "$*"; }
# ...
if [ "$FAIL" -eq 0 ]; then note "== QA PASS =="; else note "== QA FAIL =="; exit 1; fi
```

**CH 客户端调用** (`init.sh` lines 27–30):
```bash
docker compose exec clickhouse clickhouse-client \
  --user "${CH_USER:-flinklab}" --password "${CH_PASSWORD:-flinklab123}" \
  --query "SELECT 'clickhouse ok', version()"
```

**RESEARCH 推荐骨架（planner 直接采用）:**
```bash
#!/usr/bin/env bash
set -euo pipefail
COUNT=$(docker compose -f docker/docker-compose.yml exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "SELECT count() FROM flinklab.vehicle_alerts")
if [[ "${COUNT}" -lt 1 ]]; then
  echo "FAIL: expected vehicle_alerts rows >= 1, got ${COUNT}" >&2
  exit 1
fi
```

**Apply:** 至少断言 MATCH≥1（或 Kafka `vehicle.alerts` 有消息）；禁止仅 `echo OK; exit 0`。可额外检查 topic 存在（`kafka-topics --list`）。

---

### `projects/p03-vehicle-monitoring/Makefile`（config, request-response）

**Analog:** `docker/Makefile`

提供：`package`（`mvn -q clean package`）、`submit`（对齐 submit-e01）、`gen`（调 gen_vehicle_events.py）、`verify`（调 verify.sh）、可选 `up` 委托 `cd ../../docker && make up-p03`。

---

### `projects/p03-vehicle-monitoring/README.md`（config, file-I/O）

**Analogs:** `examples/e10-cep/README.md`（八段式压缩版）+ `templates/job-datastream/README.md`

**结构对齐 e10**（背景→架构/模式→验证→踩坑→实践→参考）:
- 背景与链路图（事件→CEP→Side Output→CH/Kafka）
- 启动：`make up` 基座 + `make up-p03` + package/submit/gen/verify
- 验证：`scripts/verify.sh` 失败非 0
- 踩坑：bootstrap 混用、无 within、watermark 停滞、CH flush 日志 bug
- 完整 ADR/简历页延至 Phase 3（VEH-07）；本 Phase 最小八段式即可

---

### `HarshThenFaultPatternTest.java`（test, event-driven）— Wave 0 可选

**Analog:** **无**（仓库 examples/templates 均无 `src/test` 实现类）

**依赖来源:** `templates/job-datastream/pom.xml` 已声明 `junit-jupiter` + `flink-test-utils`。

**Planner 指引:** 用 RESEARCH.md 中 Pattern API 示例写条件/within 单元测试；不替代 OrbStack e2e `verify.sh`。

---

### `docs/README.md` / `CHANGELOG.md` / `PHASES.md`

**Analog:** 自身既有约定

- `docs/README.md` 模块 15 **已登记** `projects/p03-vehicle-monitoring` — Phase 1 确认编号即可，勿另起模块号
- 会话收尾：`CHANGELOG` 未发布区 + `PHASES.md` P4 状态列；约定式提交 `feat(p4): ...` / `feat(p03): ...`

## Shared Patterns

### Compose Profile Isolation
**Source:** `docker/docker-compose.yml` lines 216–256（`profiles: ["ai"]`）  
**Apply to:** `p03-init` 及一切 p03 专用资源  
```yaml
profiles: ["p03"]
```
配合 `docker/Makefile` 的 `up-ai` 同构目标 `up-p03`；**禁止**把 p03 topic/DDL 写进 default `init.sh`。

### Kafka 双 listener 纪律
**Source:** `scripts/gen_events.py` 注释 + `JobConfig` / e01 默认  
**Apply to:** 造数脚本与 Flink 作业  
- 宿主机脚本：`localhost:9094`  
- 容器内作业：`kafka:9092`

### 算子 uid + checkpoint
**Source:** `JobTemplate.java` + `KafkaClickstreamWindowJob.java`  
**Apply to:** 所有有状态/有意义算子（source/parse/cep/sink）必须 `.uid(...)`；checkpoint 显式配置。

### CEP within + Side Output
**Source:** e10-C5 + e10-C3  
**Apply to:** Pattern 定义与 Handler；超时必须可观察。

### ClickHouse HTTP SinkV2
**Source:** e07-C6  
**Apply to:** `ClickHouseAlertSink`；修正 clear-后日志 bug；非 2xx 抛错。

### 验证失败非 0
**Source:** `scripts/qa_check.sh` + RESEARCH verify 骨架  
**Apply to:** `verify.sh`；对 CH `count()` 或 Kafka 消息数断言。

### Topic 幂等创建
**Source:** `docker/init.sh` `create_topic`  
**Apply to:** p03-init 内创建 `vehicle.events` / `vehicle.alerts`：
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vehicle.events --partitions 4 --replication-factor 1
```

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `.../HarshThenFaultPatternTest.java` | test | event-driven | 仓库无任何 `src/test` Java 用例；仅有 pom 测试依赖声明 |
| `scripts/verify.sh`（项目级 e2e 断言） | test | request-response | 无现成项目 verify；须组合 `qa_check.sh` 退出码风格 + `init.sh` CH 调用 + RESEARCH 骨架新建 |

## Metadata

**Analog search scope:** `docker/`、`templates/job-datastream/`、`examples/e01-hello-flink/`、`examples/e07-connectors/`、`examples/e10-cep/`、`scripts/`、`docs/README.md`、`CHANGELOG.md`、`PHASES.md`  
**Files scanned:** ~40（含 compose/Makefile/init/SQL/Java/Python/QA）  
**Pattern extraction date:** 2026-07-18  
**Focus analogs confirmed:** ai profile、e10-C5/C3、e07-C6、job-datastream 模板、gen_events / qa_check / init.sh
