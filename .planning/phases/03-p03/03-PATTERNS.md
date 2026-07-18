# Phase 3: p03 大盘与演练收官 - Pattern Map

**Mapped:** 2026-07-18
**Files analyzed:** 24
**Analogs found:** 21 / 24

> Phase 3 在已交付的告警链路之上做**旁路窗口作业 + Grafana 双数据源 + 演练脚本 + 文档包**。  
> 优先复用 **同项目** `VehicleAlertJob` / `ClickHouseAlertSink` / `verify.sh` / `gen_vehicle_events.py`；窗口聚合对齐 e01；压测速率对齐 `scripts/gen_events.py`；baseline 结构对齐 `benchmark/README.md`。

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `VehicleWindowMetricsJob.java` | service | streaming | `VehicleAlertJob`（Kafka→parse→WM）+ `KafkaClickstreamWindowJob`（tumbling） | exact |
| `window/EventCountAgg.java`（或同包累加器） | service | transform | `HelloEventTimeWindowJob.CountAgg` / `e02/support/CountAgg` | exact |
| `window/AttachWindowMeta.java`（ProcessWindowFunction） | service | transform | `HelloEventTimeWindowJob.AttachWindow` / `KafkaClickstreamWindowJob.ToJsonResult` | exact |
| `model/WindowMetricsRow.java`（或 window 包 POJO） | model | transform | `model/VehicleEvent.java` / `model/AlertEvent.java` | role-match |
| `sink/ClickHouseWindowMetricsSink.java` | service | streaming | `sink/ClickHouseAlertSink.java` | exact |
| `sql/clickhouse_window_metrics.sql` | migration | file-I/O | `sql/clickhouse_alerts.sql` | exact |
| `docker/docker-compose.yml`（p03-init 第 3 POST + grafana volumes/env） | config | batch | 同文件 `p03-init` wget 块 + `grafana` service | exact |
| `JobConfig.java`（window group-id / job-name 默认） | config | request-response | **自身** `JobConfig` | exact |
| `Makefile`（submit-window / loadtest / drill） | config | batch | **自身** `Makefile` `submit` / `verify-switch` | exact |
| `scripts/gen_vehicle_events.py`（`--rate`/`--duration`/stall） | utility | pub-sub | **自身** + `scripts/gen_events.py`（`--eps` 循环） | exact |
| `scripts/verify_dashboard.sh` | test | request-response | `scripts/verify.sh` + `scripts/smoke_profile.sh` | role-match |
| `scripts/loadtest.sh` | test | batch | `Makefile` `verify-switch` 等待环 + Prom curl（RESEARCH） | partial |
| `scripts/drill_watermark_stall.sh` | test | event-driven | `verify.sh`（CH 断言）+ `gen` 尾心跳纪律 + docs/02 | partial |
| `scripts/verify.sh` | test | request-response | **自身**（保持 CH CEP 权威；勿改权威语义） | exact |
| `monitoring/dashboards/p03-vehicle-overview.json` | config | file-I/O | 仓库无既有 dashboard JSON | **none** |
| `docker/.../datasources/clickhouse.yml` | config | request-response | `docker/.../datasources/datasources.yml` | role-match |
| `docker/.../dashboards/dashboards.yml` | config | file-I/O | RESEARCH Pattern 1 官方 shape（仓库无既有 provider） | partial |
| `docs/ARCHITECTURE.md` | config | file-I/O | `README.md` §2 架构 ASCII + RESEARCH 总图 | role-match |
| `docs/adr/0001-cep-broadcast-precompiled.md` | config | file-I/O | 根 `README.md` ADR-001 叙事 + `docs/PATTERN-LIBRARY.md` | role-match |
| `docs/RESUME.md` | config | file-I/O | `README.md` 硬验收列表（可验证动词） | role-match |
| `docs/baseline.md` | config | file-I/O | `benchmark/README.md` 报告模板子集 | exact |
| `README.md`（p03 八段式回填） | config | file-I/O | **自身** README | exact |
| `docs/README.md`（15-03 完成态） | config | file-I/O | 同文件 15-03 行 | exact |
| 根 `README.md`（版本矩阵登记插件） | config | file-I/O | 同文件「版本矩阵」表 | exact |
| `src/test/.../EventCountAggTest.java` | test | transform | `PatternActivationGateTest`（轻量 JUnit 契约） | role-match |

**Explicitly unchanged (do not mutate gate/CEP graph):** `VehicleAlertJob` CEP+Broadcast 语义、`PatternActivationGate`、三 Pattern 工厂、`verify.sh` 以 CH `pattern_id` 为 CEP 权威出口。

## Pattern Assignments

### `VehicleWindowMetricsJob.java`（service, streaming）

**Analogs:**
1. 源/WM/uid：`VehicleAlertJob.java`
2. 窗口图：`KafkaClickstreamWindowJob.java`

**Kafka source + parse-after + WM（与告警作业同策略）** (`VehicleAlertJob.java` lines 63–81):
```java
KafkaSource<String> eventSource = KafkaSource.<String>builder()
        .setBootstrapServers(cfg.kafkaBootstrap)
        .setTopics(cfg.eventsTopic)
        .setGroupId(cfg.groupId)  // MUST differ: e.g. p03-window-metrics
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<VehicleEvent> events = env
        .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-vehicle-events")
        .uid("p03-wm-source-vehicle-events")
        .flatMap(new ParseVehicleJson())
        .name("parse-vehicle-json")
        .uid("p03-wm-parse-vehicle-json")
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTime)
                        .withIdleness(Duration.ofSeconds(30)));
```

**Tumbling window + aggregate** (`KafkaClickstreamWindowJob.java` lines 84–88):
```java
clicks.keyBy(e -> e.page)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .aggregate(new PvUvAgg(), new ToJsonResult())
        .name("pv-uv-1m")
        .uid("e01-j2-window");
```

**Apply to window job:**
```java
events.keyBy(e -> e.vin)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1))) // or 10–30s if loadtest short (A3)
    .aggregate(new EventCountAgg(), new AttachWindowMeta())
    .name("vin-window-metrics")
    .uid("p03-wm-window-agg")
    .sinkTo(new ClickHouseWindowMetricsSink(cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
    .name("clickhouse-window-metrics")
    .uid("p03-wm-sink-clickhouse");
```

**Env/checkpoint skeleton** — copy `VehicleAlertJob.main` (`JobConfig.from` + RocksDB + EXACTLY_ONCE)。  
**Pitfall:** 独立 `group.id`（`JobConfig` 默认 `p03-window-metrics`），禁止与告警作业共享。

---

### `window/EventCountAgg.java` + `AttachWindowMeta`（service, transform）

**Analog:** `HelloEventTimeWindowJob.java` lines 67–85

**CountAgg:**
```java
static final class CountAgg implements AggregateFunction<ClickEvent, Long, Long> {
    @Override public Long createAccumulator()                { return 0L; }
    @Override public Long add(ClickEvent v, Long acc)        { return acc + 1; }
    @Override public Long getResult(Long acc)                { return acc; }
    @Override public Long merge(Long a, Long b)              { return a + b; }
}
```

**AttachWindow（贴窗口元信息）:**
```java
static final class AttachWindow
        extends ProcessWindowFunction<Long, String, String, TimeWindow> {
    @Override
    public void process(String page, Context ctx, Iterable<Long> counts, Collector<String> out) {
        long c = counts.iterator().next();
        out.collect("window[%s ~ %s] page=%s clicks=%d".formatted(
                Instant.ofEpochMilli(ctx.window().getStart()),
                Instant.ofEpochMilli(ctx.window().getEnd()),
                page, c));
    }
}
```

**Apply:** 累加器建议用多字段 POJO（`event_count` / `harsh_count` / `dtc_count`），形态对齐 `KafkaClickstreamWindowJob.PvUvAgg`（lines 134–157）的 `add`/`merge`，输出 `WindowMetricsRow`（vin + window_start/end + counts）而非 String。

**多计数累加器参考** (`KafkaClickstreamWindowJob.java` lines 141–145):
```java
public PvUv add(ClickEvent e, PvUv acc) {
    acc.pv++;
    acc.users.add(e.userId);
    return acc;
}
```

p03 版：
```java
// signalType 白名单同 ParseVehicleJson：HARSH_ACCEL / DTC / HEARTBEAT
acc.eventCount++;
if ("HARSH_ACCEL".equals(e.signalType)) acc.harshCount++;
if ("DTC".equals(e.signalType)) acc.dtcCount++;
```

---

### `sink/ClickHouseWindowMetricsSink.java`（service, streaming）

**Analog:** `ClickHouseAlertSink.java`（完整 SinkV2 HTTP 批写）

**Imports / Sink 骨架** (lines 1–40):
```java
public final class ClickHouseAlertSink implements Sink<AlertEvent> {
    private final String baseUrl;
    private final String user;
    private final String password;
    // ...
    @Override
    public SinkWriter<AlertEvent> createWriter(WriterInitContext context) {
        return new BatchWriter(baseUrl, user, password);
    }
}
```

**flush + 非 2xx 抛错** (lines 61–103):
```java
public void flush(boolean endOfInput) throws IOException {
    if (buffer.isEmpty()) {
        return;
    }
    // build INSERT ... VALUES (...),(...);
    String sql = "INSERT INTO flinklab.vehicle_alerts"
            + "(vin,alert_type,pattern_id,signal_summary,harsh_value,fault_value,event_time) VALUES "
            + body;
    // HttpURLConnection POST → baseUrl?user=&password=
    // code < 200 || code >= 300 → throw IOException
}
```

**Validation 纪律** (lines 110–133): 拒绝 `null` / 引号 / 反斜杠；表名列名常量；whitelist 枚举。

**Apply:** 克隆为 `ClickHouseWindowMetricsSink`，表改为 `flinklab.vehicle_window_metrics`，列对齐 DDL；`vin` 仍做 `containsForbidden`；计数字段用 `%d` 而非字符串拼接未校验用户输入。

---

### `sql/clickhouse_window_metrics.sql` + compose `p03-init`（migration / config）

**DDL analog:** `sql/clickhouse_alerts.sql` lines 1–15:
```sql
-- 注意：ClickHouse HTTP 默认禁多语句；CREATE 与 ALTER 由 p03-init 分两次 POST。
CREATE TABLE IF NOT EXISTS flinklab.vehicle_alerts
(
    vin            String,
    ...
    ingest_time    DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, vin)
```

**Apply (RESEARCH 建议列):**
```sql
CREATE TABLE IF NOT EXISTS flinklab.vehicle_window_metrics
(
    vin           String,
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    event_count   UInt64,
    harsh_count   UInt64,
    dtc_count     UInt64,
    ingest_time   DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (window_start, vin)
```

**p03-init 第三 POST analog** (`docker/docker-compose.yml` lines 303–333):
```yaml
volumes:
  - ../projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql:/ddl/clickhouse_alerts.sql:ro
  - ../projects/p03-vehicle-monitoring/sql/clickhouse_alerts_alter.sql:/ddl/clickhouse_alerts_alter.sql:ro
  # NEW:
  # - ../projects/p03-vehicle-monitoring/sql/clickhouse_window_metrics.sql:/ddl/clickhouse_window_metrics.sql:ro
# command 内追加第三次:
# wget -qO- --post-file=/ddl/clickhouse_window_metrics.sql \
#   "http://clickhouse:8123/?user=$${CH_USER}&password=$${CH_PASSWORD}"
```

**禁止**把 metrics DDL 并进 alerts 单文件多语句 POST。

---

### `JobConfig.java`（config）

**Analog:** **自身** lines 51–65

```java
public static JobConfig from(String[] args) {
    return new JobConfig(
            arg(args, "job-name", "p03-vehicle-alert"),
            ...
            arg(args, "group-id", "p03-vehicle-alerts"),
            arg(args, "clickhouse-url", "http://clickhouse:8123/"),
            ...
    );
}
```

**Apply:** 窗口作业 `main` 传不同默认，或新增可选 key：
- `--job-name` 默认 `p03-vehicle-window-metrics`
- `--group-id` 默认 `p03-window-metrics`  
勿改告警作业现有默认（回归风险）。

---

### `Makefile`（submit-window / drills）

**Analog:** `projects/p03-vehicle-monitoring/Makefile` lines 16–26 (`submit`) + 42–61 (`verify-switch` 等待环)

```makefile
submit: package
	@JAR=$$(ls target/p03-vehicle-monitoring-*.jar 2>/dev/null | grep -v original | head -1); \
	...
	cd ../../docker && docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p03.VehicleAlertJob \
		"/opt/flink/usrlib/$$JAR_NAME"
```

**Apply:**
```makefile
submit-window: package
	# 同 jar 复制路径；-c com.flywhl.flinklab.p03.VehicleWindowMetricsJob
loadtest:
	bash scripts/loadtest.sh
drill-watermark:
	bash scripts/drill_watermark_stall.sh
verify-dashboard:
	bash scripts/verify_dashboard.sh
```

等待/轮询风格复用 `verify-switch` 的 `deadline` + `sleep 3` 环。

---

### `scripts/gen_vehicle_events.py`（utility, pub-sub）

**Analogs:**
1. 场景/尾心跳：**自身** `_tail_heartbeats` + scenario 函数
2. 恒定速率：`scripts/gen_events.py` `--eps` 循环

**尾心跳** (gen_vehicle_events.py lines 110–120):
```python
def _tail_heartbeats(vin: str, base: int, offsets: tuple[int, ...] = (12_000, 18_000)) -> list[dict]:
    """尾心跳推进 watermark：须把水位推过末业务事件（ooo=5s）。"""
    return [
        {"vin": vin, "signalType": "HEARTBEAT", "value": 1.0, "eventTime": base + off}
        for off in offsets
    ]
```

**速率循环** (`scripts/gen_events.py` lines 66–80):
```python
while running and (args.total == 0 or sent < args.total):
    batch_deadline = tick + 1.0
    for _ in range(args.eps):
        event = {
            "userId": f"u{random.randint(1, args.users)}",
            "page": page,
            "ts": int(time.time() * 1000) - random.randint(0, args.max_lag_ms),
        }
        producer.produce(args.topic, json.dumps(event).encode(), key=...)
```

**Apply:**
- 增加 `--rate`（或别名 `--eps`）+ `--duration`；与 `--scenario` 互斥或组合规则写清
- stall 模式：固定 `eventTime=T0` 的 HEARTBEAT 涓流 ≥45s（RESEARCH Pattern 5）；**不要**只靠空闲分区（idleness=30s 会剔出空分区）
- recover：推进时间戳的 DTC/完整 scenario + `_tail_heartbeats`
- 保留 uv script header + `localhost:9094` 默认

---

### `scripts/verify_dashboard.sh`（test, request-response）

**Analogs:** `verify.sh`（fail 非 0 + 白名单）+ `smoke_profile.sh`（compose 断言）

**verify.sh 骨架** (lines 1–20, 53–80):
```bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
# ...
if [[ "${MATCH_COUNT}" -lt "${MIN_COUNT}" ]]; then
  echo "FAIL: ..." >&2
  exit 1
fi
echo "ok ..."
```

**Apply (RESEARCH Code Examples):**
```bash
# 1) JSON 文件非空
test -s "${REPO_ROOT}/projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json"
# 2) Grafana API
curl -sf -u admin:flinklab http://localhost:3000/api/datasources \
  | grep -q grafana-clickhouse-datasource || grep -q Prometheus
curl -sf -u admin:flinklab 'http://localhost:3000/api/search?query=p03' \
  | grep -q p03-vehicle
# 可选：CH metrics 表 count>=1（VEH-05 smoke）
```

插件未装时：文档化 fallback；脚本可对 CH DS 做 soft-fail 或明确 FAIL（planner 定门禁严格度）。

---

### `scripts/loadtest.sh`（test, batch）

**Analogs:** `Makefile` `verify-switch` 编排；Prom scrape 无仓库脚本 → 用 RESEARCH 已验证 curl

```bash
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=flink_jobmanager_job_lastCheckpointDuration{job_name=~"p03.*"}'
```

**指标名** 对齐 `monitoring/README.md` 值班五指标（反压 / busy / ckpt / restarts / event-time lag）。

**输出：** 写入 `docs/baseline.md`，结构对齐 `benchmark/README.md` lines 5–8：
1. 环境快照  
2. 负载定义（eps/时长/并行度）  
3. 指标表  
4. 结论  

热身 30–60s 声明丢弃（D-08）；**禁止**写未实测 SLA。

---

### `scripts/drill_watermark_stall.sh`（test, event-driven）

**Analogs:**
- CH 断言：`verify.sh`
- 造数：`gen_vehicle_events.py` scenario + 尾心跳
- 理论：`docs/02-time-window/README.md`（idleness 救部分空闲、不救全空闲）

**剧本两阶段（RESEARCH Pattern 5）：**
1. Stall：部分 CEP 序列 + **冻结 eventTime** HEARTBEAT ≥45s → 断言 MATCH 不增 + Prom `currentEmitEventTimeLag` 上升  
2. Recover：完成序列 + advancing 尾心跳 → `PATTERN_ID=… bash scripts/verify.sh`  

**禁止**仅依赖 Flink REST `/watermarks`（live 返回 `[]`）。UI Watermarks 列作 checklist 副证。

---

### Grafana provisioning（config）

#### Datasource YAML

**Analog:** `docker/config/grafana/provisioning/datasources/datasources.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

**Apply — NEW `clickhouse.yml`**（RESEARCH Pattern 2）:
```yaml
apiVersion: 1
datasources:
  - name: ClickHouse
    uid: p03-clickhouse
    type: grafana-clickhouse-datasource
    access: proxy
    jsonData:
      host: clickhouse
      port: 8123
      protocol: http
      defaultDatabase: flinklab
      username: flinklab
    secureJsonData:
      password: flinklab123
```

#### Dashboards provider

**无仓库既有文件** — 用 RESEARCH Pattern 1 官方 shape（见 RESEARCH.md）。挂载：
```yaml
# grafana service 追加
environment:
  GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
volumes:
  - ./config/grafana/provisioning:/etc/grafana/provisioning:ro
  - ../projects/p03-vehicle-monitoring/monitoring/dashboards:/var/lib/grafana/dashboards/p03:ro
```

**Dashboard JSON：** 顶层 dashboard document（非 API wrapper）；面板最少：窗口吞吐、pattern_id 告警速率、异常阈值、Flink 健康子集。PromQL 抄 `monitoring/README.md` 五指标名。

---

### Docs pack（ARCHITECTURE / ADR / RESUME / baseline）

| File | Analog | Copy what |
|------|--------|-----------|
| `docs/ARCHITECTURE.md` | p03 `README.md` §2 ASCII 图 | 扩展为「告警 + 窗口大盘 + 演练」；交叉引用 docs/10-cep、docs/02-time-window、monitoring/ |
| `docs/adr/0001-cep-broadcast-precompiled.md` | 根 README ADR-001 + PATTERN-LIBRARY Broadcast 节 | 锁定「编译期 Pattern + Broadcast 激活集」vs 商业动态 CEP；可验证路径指向 PatternIds/Gate |
| `docs/RESUME.md` | README「硬验收」列表 | 可验证动词 + 指向 verify/baseline/大盘；数字只引实测 |
| `docs/baseline.md` | `benchmark/README.md` 模板 | 环境→负载→指标表→结论；热身声明 |

**ADR-001 叙事密度参考**（根 `README.md` lines 40–41）：问题 → 生态约束 → 决策 → 不做什么。

**PATTERN-LIBRARY Broadcast 纪律**（`docs/PATTERN-LIBRARY.md` lines 20–21）可原文写入 ADR「后果」节：门控关闭不释放 CEP 状态。

---

### `docs/README.md` 15-03 + 根版本矩阵

**15-03 现状** (`docs/README.md` line 73)：🚧 Phase 2… Grafana/压测延后续 → 改为完成态表述（大盘 JSON + baseline + ADR + RESUME 可打开）。

**根 README 版本矩阵**：新增行登记 `grafana-clickhouse-datasource`（via `GF_INSTALL_PLUGINS`），禁止未登记先用（ENG-01）。

---

### `EventCountAggTest.java`（test, transform）

**Analog:** `PatternActivationGateTest.java` lines 20–35 — 轻量 JUnit、无 MiniCluster：

```java
class PatternActivationGateTest {
    @Test
    void emptyStateDefaultsToHarshThenFaultOnly() {
        Set<String> active = PatternActivationGate.resolveActivePatterns(null);
        assertEquals(Set.of(PatternIds.HARSH_THEN_FAULT), active, "...");
    }
}
```

**Apply:** 对累加器 `createAccumulator` / `add`（HEARTBEAT vs HARSH vs DTC）/ `merge` 做契约断言；Nyquist Wave 0 便宜 RED→GREEN。

---

## Shared Patterns

### ClickHouse = CEP 权威出口
**Source:** `scripts/verify.sh`  
**Apply to:** `drill_watermark_stall.sh` recover 断言；勿用 Kafka alerts 放行  
```bash
CH_MATCH_QUERY="SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH' AND pattern_id='${PATTERN_ID}'"
# PATTERN_ID 白名单 case 后才拼 SQL
```

### Watermark / idleness 纪律
**Source:** `VehicleAlertJob` WM 块 + `docs/02-time-window` + `_tail_heartbeats`  
**Apply to:** `VehicleWindowMetricsJob`、stall drill、loadtest recover  
- ooo=5s、idleness=30s  
- 推进水位用 advancing HEARTBEAT；stall 用 **frozen eventTime** 涓流  

### HTTP SinkV2 安全写入
**Source:** `ClickHouseAlertSink.validate`  
**Apply to:** `ClickHouseWindowMetricsSink`  
- 表/列常量；拒绝 `'"` `\`；非 2xx 抛错  

### 算子 `.uid(...)` + 独立 group.id
**Source:** `VehicleAlertJob` / `KafkaClickstreamWindowJob`  
**Apply to:** 窗口作业全部算子；`group-id=p03-window-metrics`  

### Shell 脚本契约
**Source:** `verify.sh` / `smoke_profile.sh`  
**Apply to:** verify_dashboard / loadtest / drill  
```bash
set -euo pipefail
# FAIL: ... >&2; exit 1
# ok ...
```

### Grafana provisioning 挂载
**Source:** compose `grafana` volumes + RESEARCH Pattern 1–2  
**Apply to:** datasources + dashboards provider；canonical JSON 在 p03 `monitoring/dashboards/`；default `make up` **不加** `--profile p03`

### Baseline 报告口径
**Source:** `benchmark/README.md`  
**Apply to:** `docs/baseline.md` / `loadtest.sh`  
环境快照 → 负载 → 指标表 → 结论；热身丢弃写明；无实测数字不上文档  

### 文档八段式 / 编号登记
**Source:** p03 `README.md` + `docs/README.md`  
**Apply to:** README 回填验证/演练节；15-03 完成态；禁 TODO/省略/略  

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `monitoring/dashboards/p03-vehicle-overview.json` | config | file-I/O | 仓库尚无任何 Grafana dashboard JSON；按 RESEARCH 面板清单 + Grafana 11 provisioning document shape 新建 |
| `docker/.../dashboards/dashboards.yml` | config | file-I/O | 仅有 datasources.yml；provider YAML 用 RESEARCH Pattern 1 官方模板 |
| `scripts/loadtest.sh`（完整 Prom→baseline 写文件） | test | batch | 无现成压测脚本；编排可抄 verify-switch，PromQL 抄 monitoring README |

Planner 对上述三项以 **03-RESEARCH.md** Code Examples / Pattern 1–6 为权威。

## Metadata

**Analog search scope:**  
`projects/p03-vehicle-monitoring/` · `examples/e01-hello-flink/` · `examples/e02-time-window/` · `scripts/gen_events.py` · `docker/docker-compose.yml` · `docker/config/grafana/` · `monitoring/` · `benchmark/` · `docs/` · 根 `README.md`

**Files scanned:** ~45（含 p03 全树 + e01 窗口作业 + compose/grafana/monitoring/benchmark）  
**Strong analogs used:** 5 主簇（AlertJob/Sink、e01 window、verify/gen、compose p03-init、benchmark/monitoring docs）  
**Pattern extraction date:** 2026-07-18
