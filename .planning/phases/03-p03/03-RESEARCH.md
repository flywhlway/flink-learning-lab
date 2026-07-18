# Phase 3: p03 大盘与演练收官 - Research

**Researched:** 2026-07-18
**Domain:** Grafana dual-datasource dashboards · Flink window-agg side job · loadtest/watermark-stall drills · p03 doc pack
**Confidence:** HIGH

## Summary

Phase 3 closes p03 to PHASES P4 single-project complete: Grafana dashboard (window metrics + anomaly threshold panels + Flink health), two executable drills (watermark stall + load baseline) with OrbStack numbers in `docs/baseline.md`, and the VEH-07 doc pack (ARCHITECTURE, ADR-0001 CEP+Broadcast, RESUME, scriptable verify extensions). Locked CONTEXT (D-01–D-15) already removes most stack debates; research focuses on *how* to implement those decisions against the live compose (Grafana 11.1.0 + Prometheus-only provisioning today, ClickHouse 24.8, Flink 2.2.1, `vehicle.events` 4 partitions, `withIdleness(30s)`).

**Primary recommendation:** Add a separate DataStream tumbling-window job writing `flinklab.vehicle_window_metrics` via existing ClickHouse HTTP SinkV2 pattern; provision Grafana dashboards + official `grafana-clickhouse-datasource` (HTTP → `clickhouse:8123`) alongside existing Prometheus; extend `gen_vehicle_events.py` with `--rate/--duration`; ship `loadtest.sh` + `drill_watermark_stall.sh` that scrape Prometheus + assert CH; keep `VehicleAlertJob` graph untouched and CH as CEP authority.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** 大盘采用 **双数据源**：业务窗口指标落 **ClickHouse 指标表**（Grafana ClickHouse 或 Prometheus 旁路均可，以可导入 JSON + 本机可打开为准）；平台健康沿用已接通的 **Prometheus → Flink reporter**（`monitoring/` 值班五指标）。禁止「只有截图、无可导入 dashboard JSON」。
- **D-02:** 窗口聚合与 CEP 告警作业 **解耦**：在同一 `vehicle.events` 上新增轻量 **窗口聚合作业**（DataStream 或 Table/SQL，由 researcher 按现有 SSOT 选型），输出按 vin/窗口的计数类指标写入 CH；**不**把大盘聚合逻辑塞进 `VehicleAlertJob` 主图。
- **D-03:** Grafana dashboard JSON **provisioning 落仓**（`docker/config/grafana/provisioning/dashboards/` + p03 专用 JSON，或 `projects/p03-vehicle-monitoring/monitoring/` 再挂载——保持 default `make up` 不被 p03 业务面板硬绑定；推荐 p03 profile / 文档一键导入路径二选一，planner 须保证 OrbStack 上可打开）。面板最少覆盖：窗口吞吐/事件计数、按 `pattern_id` 的告警速率、异常阈值面板、Flink 健康（反压/checkpoint/重启/event-time lag 子集）。
- **D-04:** 「异常检测规则」落地为 **Grafana 阈值/告警面板**（例如单位时间 MATCH 激增、某 vin 急加速计数越界），规则条文写进 p03 文档（可读、可复现阈值）；**不**新增独立 ML 异常作业，**不**再扩 CEP 模式库条目来「假装异常检测」。
- **D-05:** 阈值数字必须来自本机演练可观察量或文档明示的演示默认值；禁止未实测却写成生产 SLA。
- **D-06:** 本 Phase 压测为 **p03 项目级**（固定 eps/时长/并行度快照下的吞吐、lag、checkpoint 摘要），产出 `projects/p03-vehicle-monitoring/docs/baseline.md`；**不**实现 `benchmark/` 全矩阵（PROD-01 / Phase 6）。方法论可交叉引用 `benchmark/README.md`，数字只写 OrbStack arm64 实测。
- **D-07:** 压测驱动优先 **扩展现有** `scripts/gen_vehicle_events.py`（`--rate` / `--duration` 或等价）+ `make`/`scripts/` 包装拉取 Prometheus/CH 摘要；**不**为 p03 引入未登记的 k6/JMeter 镜像（P5 再评估）。
- **D-08:** baseline 报告模板对齐 `benchmark/README.md` 口径子集：环境快照 → 负载定义 → 指标表 → 结论；热身可缩短但须写明（不必强制 3 分钟，学习工程可 30–60s 热身，须在 baseline 中声明）。
- **D-09:** 可执行演练 **恰好 2 条**（MVP）：(1) **watermark 停滞**（必做）；(2) **负载压测跑 baseline**（与 D-06 同一路径）。额外 chaos（杀 TM、断 Kafka）可写「可选附录」，不挡 Phase 完成。
- **D-10:** watermark 停滞剧本：通过造数/分区空闲制造水位停摆（对齐 docs/02-time-window 与作业已有 `withIdleness(30s)`），在 Flink UI Watermarks 列留下可观察证据，再恢复生产/尾心跳推进水位；脚本或 checklist 须断言「停滞可观察 → 恢复后 MATCH/窗口可继续」。禁止只写散文无命令。
- **D-11:** 演练入口放在 `projects/p03-vehicle-monitoring/scripts/`（如 `drill_watermark_stall.sh` + `loadtest.sh`）并由 README「验证/演练」节一键可跟；失败非 0。
- **D-12:** 至少 **1 篇 ADR**：主题锁定 **「开源 CEP：编译期 Pattern + Broadcast 选择预编译激活集」vs 商业动态 CEP / 运行时编译**（承接 Phase 2 D-07 与 STACK 拒绝项）。路径：`projects/p03-vehicle-monitoring/docs/adr/0001-cep-broadcast-precompiled.md`（编号可由 planner 微调，须可按路径打开）。
- **D-13:** **简历陈述页** `projects/p03-vehicle-monitoring/docs/RESUME.md`：用可验证动词 + 指向 verify/baseline/大盘路径；禁止空泛形容词；数字只引用 baseline/verify 实测。
- **D-14:** **架构文档**：在 p03 README 架构节补全「告警 + 窗口大盘 + 演练」总图，或独立 `docs/ARCHITECTURE.md`（二选一，推荐独立短文 + README 链接）；回填 `docs/README.md` 模块 15-03 状态为完成态表述；交叉引用 docs/10-cep、docs/02-time-window、monitoring/。
- **D-15:** 验证脚本保持 **ClickHouse 为 CEP 告警权威出口**（Phase 1–2 纪律）；大盘/压测另有断言（dashboard 文件存在 + Grafana HTTP 或 provisioning 校验、baseline 文件非空且含实测表）——具体门禁由 planner 写入 verify/qa 扩展，须可脚本化。

### Claude's Discretion
- 窗口聚合作业类名、CH 指标表 DDL 列设计、Grafana JSON 面板布局细节、ClickHouse vs Prometheus 作为业务面板主查询引擎的最终选型（只要满足 D-01–D-03 可导入可打开）。
- 压测默认 eps/时长在 OrbStack 可稳定跑通前提下由 executor 实测填写；CONTEXT 不锁死具体数字。

### Deferred Ideas (OUT OF SCOPE)
- `benchmark/` 全矩阵 + 仓库级 `baseline.md` → Phase 6（PROD-01）
- monitoring 三块看板终稿（平台总览 / 作业深潜 / AI 专项）→ Phase 6（PROD-04）
- Loki / OTel Tracing → P5/P6 规划，本 Phase 不做
- k6 / 商业压测工具镜像 → 未进 SSOT 前不做
- 杀 TM / 断网等额外 chaos → 可选附录，不挡 VEH-06
- p01/p02 复制本 Phase 纪律 → Phase 4 / 5
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| VEH-05 | 维护者可复现监控大盘（窗口聚合指标 + Grafana + 异常检测规则） | Dual DS + separate window job + CH metrics table + Grafana threshold panels (D-01–D-05); provisioning YAML pattern below |
| VEH-06 | 压测脚本与故障演练（含 watermark 停滞）可执行，并记录 baseline 数字 | Extend gen + `loadtest.sh` / `drill_watermark_stall.sh`; Prometheus scrape; baseline template from `benchmark/README.md` subset |
| VEH-07 | 交付架构文档、至少 1 篇 ADR、验证脚本与简历陈述页 | ADR path locked; ARCHITECTURE + RESUME; extend verify for dashboard/baseline gates; CH remains CEP authority |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| CEP alert matching + Broadcast gate | Flink Job (`VehicleAlertJob`) | ClickHouse `vehicle_alerts` | Already delivered; Phase 3 must not mutate gate semantics |
| Window aggregation metrics | Flink Job (new side job) | ClickHouse `vehicle_window_metrics` | D-02: decouple from CEP graph; CH is business store |
| Platform health metrics | Flink Prometheus reporter → Prometheus | Grafana Prometheus panels | Already wired (`:9249`, `monitoring/` 五指标) |
| Anomaly “rules” | Grafana threshold panels + p03 docs | — | D-04: not ML, not more CEP |
| Load generation | Host Python (`gen_vehicle_events.py`) | Kafka `vehicle.events` | D-07: extend existing; no k6 |
| Drill / baseline assertions | Shell scripts under `p03/scripts/` | Prometheus HTTP + CH SQL + Grafana HTTP | D-11/D-15; fail non-zero |
| Dashboard delivery | Grafana provisioning / API | Git JSON under p03 `monitoring/` | D-03: importable JSON, default `make up` not p03-profile-bound |
| Doc pack / resume | Repo markdown | `docs/README.md` 15-03 | VEH-07 / D-12–D-14 |

## Project Constraints (from .cursor/rules/)

No `.cursor/rules/` directory present in this repo. Effective constraints come from `CLAUDE.md` / PROJECT invariants:

- Version SSOT: root README matrix + pom properties; register before use
- Docs: register in `docs/README.md`; 八段式 where applicable
- OrbStack arm64 verification required; no fake “ran green”
- Forbidden prose: TODO / 省略 / 略 / 自行实现 / 请参考官网
- Independent p03 pom; default `make up` must not require `--profile p03`
- ClickHouse remains CEP verify authority

## Standard Stack

### Core

| Library / Component | Version | Purpose | Why Standard |
|---------------------|---------|---------|--------------|
| Apache Flink | 2.2.1 | Window side job + existing CEP job | SSOT / ADR-001; `[VERIFIED: docker/.env + p03 pom]` |
| JDK | 21 | Compile/run | SSOT; `[VERIFIED: java -version 21.0.2]` |
| flink-connector-kafka | 5.0.0-2.2 | Consume `vehicle.events` in window job | Same as alert job; `[VERIFIED: p03 pom]` |
| ClickHouse | 24.8 | `vehicle_alerts` + new metrics table | Base compose; `[VERIFIED: docker/.env]` |
| Prometheus | v2.53.0 | Flink health scrape | `[VERIFIED: docker/.env]` |
| Grafana | 11.1.0 | Dashboard UI + provisioning | `[VERIFIED: docker/.env + live :3000]` |
| grafana-clickhouse-datasource | latest compatible with Grafana 11.x (install via `GF_INSTALL_PLUGINS`) | Query CH business panels | Official Grafana Labs plugin; `[CITED: grafana.com/docs/plugins/grafana-clickhouse-datasource]` |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| ClickHouse HTTP SinkV2 (project-local) | reuse `ClickHouseAlertSink` pattern | Write window rows | Mirror e07-C6 / existing alert sink; no new Maven coord |
| `confluent-kafka` (uv script) | ≥2.5 (existing) | Load / drill produce | Extend gen only; `[VERIFIED: gen_vehicle_events.py header]` |
| Flink DataStream `TumblingEventTimeWindows` | Flink 2.2.1 | Window agg | Align e01 `HelloEventTimeWindowJob` / `KafkaClickstreamWindowJob` |
| Grafana file provisioning | Grafana 11.x | Auto-load dashboard JSON | `[CITED: grafana.com/docs/grafana/latest/administration/provisioning/#dashboards]` |
| Prometheus HTTP API | v2.53 | loadtest metric scrape | Live verified on OrbStack |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Grafana ClickHouse plugin for business panels | Prometheus-only + Flink custom `MetricGroup` | Avoids plugin install; weakens D-01 “CH 指标表可在大盘查询”; keep as **fallback** if plugin install fails on arm64 |
| DataStream window job | Flink Table/SQL window | Would add `flink-table-*` deps to independent p03 pom; not in current pom — **reject for Phase 3** |
| k6 / JMeter | Extended `gen_vehicle_events.py` | Forbidden by D-07 until SSOT |
| Mount p03 dashboards only under profile | Always-on provider path + empty panels OR Grafana API import script | Profile cannot remount running Grafana easily; prefer always-on JSON mount that does not depend on `p03-init` |

**Installation (compose / Maven — no new Maven Central artifacts expected):**

```bash
# Grafana plugin (compose env on grafana service) — must register in root README 版本矩阵 first
# GF_INSTALL_PLUGINS=grafana-clickhouse-datasource

# Window job: same p03 jar, second main class
cd projects/p03-vehicle-monitoring && mvn -q clean package
# flink run -c com.flywhl.flinklab.p03.VehicleWindowMetricsJob ...
```

**Version verification (this session):**

| Component | Observed |
|-----------|----------|
| Grafana image | `grafana/grafana:11.1.0` |
| Prometheus | `prom/prometheus:v2.53.0` |
| ClickHouse | `clickhouse/clickhouse-server:24.8` (HTTP 8123, native host-map 9002→9000) |
| Live datasources | Prometheus only (`uid` PBFA97CFB590B2093); **no** ClickHouse plugin installed yet |
| Flink job running | `p03-vehicle-alert` RUNNING; Prom scrapes `currentEmitEventTimeLag`, `lastCheckpointDuration`, `numRestarts` |

## Package Legitimacy Audit

> Phase installs **no new npm/PyPI/crates packages**. Loadgen stays on existing uv `confluent-kafka`. Maven stays on existing Flink/Jackson coords. The only external add is the **Grafana plugin** `grafana-clickhouse-datasource` via official `GF_INSTALL_PLUGINS` (not a registry language package).

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| grafana-clickhouse-datasource | Grafana plugin catalog | years (Grafana Labs) | n/a (plugin) | github.com/grafana/clickhouse-datasource | n/a (not npm/pypi) | Approved — install via GF_INSTALL_PLUGINS; register in README SSOT |
| confluent-kafka | PyPI | mature | high | github.com/confluentinc/confluent-kafka-python | not re-run (already in-repo) | Keep existing pin in uv script |
| *(no new Maven artifacts)* | Maven Central | — | — | — | — | N/A |

**Packages removed due to slopcheck [SLOP] verdict:** none  
**Packages flagged as suspicious [SUS]:** none  
**slopcheck note:** Available (0.6.1); no new installable language packages to score. Plugin legitimacy grounded in official Grafana/ClickHouse docs, not training data alone.

## Architecture Patterns

### System Architecture Diagram

```text
                    ┌─────────────────────────────┐
                    │ gen_vehicle_events.py       │
                    │  (--scenario | --rate/--dur │
                    │   | stall/recover modes)    │
                    └──────────────┬──────────────┘
                                   │ produce
                                   ▼
                          Kafka vehicle.events
                         (4 partitions, p03-init)
                     ┌─────────────┴──────────────┐
                     │                            │
                     ▼                            ▼
        VehicleAlertJob (unchanged)     VehicleWindowMetricsJob (NEW)
        WM: ooo=5s, idleness=30s        same WM strategy (recommended)
        3× CEP → Gate → dual sink       keyBy(vin) → tumbling window
                     │                  → CH HTTP SinkV2
                     ▼                            │
        CH flinklab.vehicle_alerts                ▼
        (CEP authority / pattern_id)   CH flinklab.vehicle_window_metrics
                     │                            │
                     └────────────┬───────────────┘
                                  ▼
                     Grafana (provisioned JSON)
              ┌───────────────────┴────────────────────┐
              │ Prometheus DS (existing)               │ ClickHouse DS (NEW plugin)
              │ Flink health: backpressure, ckpt,      │ window counts, alert rate SQL,
              │ restarts, event-time lag               │ threshold anomaly panels
              └───────────────────┬────────────────────┘
                                  │
              loadtest.sh / drill_watermark_stall.sh
              scrape Prom + assert CH + check dashboard API
```

### Recommended Project Structure

```text
projects/p03-vehicle-monitoring/
├── src/main/java/.../p03/
│   ├── VehicleAlertJob.java              # unchanged CEP graph
│   ├── VehicleWindowMetricsJob.java      # NEW side job main
│   ├── window/                           # NEW: agg + row model
│   └── sink/ClickHouseWindowMetricsSink.java  # NEW (clone alert sink)
├── sql/
│   ├── clickhouse_alerts.sql             # existing
│   ├── clickhouse_alerts_alter.sql       # existing
│   └── clickhouse_window_metrics.sql     # NEW (p03-init POST #3)
├── monitoring/dashboards/
│   └── p03-vehicle-overview.json         # canonical dashboard JSON
├── scripts/
│   ├── gen_vehicle_events.py             # extend --rate/--duration + stall helpers
│   ├── verify.sh                         # keep CH CEP authority
│   ├── verify_dashboard.sh               # NEW: JSON + Grafana API
│   ├── loadtest.sh                       # NEW
│   └── drill_watermark_stall.sh          # NEW
└── docs/
    ├── ARCHITECTURE.md                   # recommended (D-14)
    ├── baseline.md                       # loadtest output
    ├── RESUME.md
    ├── PATTERN-LIBRARY.md                # existing
    └── adr/0001-cep-broadcast-precompiled.md

docker/config/grafana/
├── provisioning/
│   ├── datasources/
│   │   ├── datasources.yml               # keep Prometheus
│   │   └── clickhouse.yml                # NEW ClickHouse DS
│   └── dashboards/
│       └── dashboards.yml                # NEW provider → mounted JSON path
└── dashboards/p03/                       # OR bind-mount ../projects/.../monitoring/dashboards
    └── .gitkeep
```

### Pattern 1: Grafana dashboards provider (this compose)

**What:** File provider under `/etc/grafana/provisioning/dashboards/` pointing at a mounted JSON directory.  
**When to use:** Always for VEH-05 (D-03).  
**Example** (official shape):

```yaml
# Source: https://grafana.com/docs/grafana/latest/administration/provisioning/#dashboards
apiVersion: 1
providers:
  - name: p03-vehicle
    orgId: 1
    folder: p03
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards/p03
      foldersFromFilesStructure: false
```

**Compose mount recommendation (D-03 compliance):**

1. Canonical JSON lives in `projects/p03-vehicle-monitoring/monitoring/dashboards/` (project-owned).
2. Grafana service gains an extra volume:  
   `../projects/p03-vehicle-monitoring/monitoring/dashboards:/var/lib/grafana/dashboards/p03:ro`
3. This does **not** start `p03-init` and does **not** change `make up` profile flags — empty/no-data panels are fine until jobs run. That satisfies “default make up 不被 p03 业务面板硬绑定” better than requiring `--profile p03` for Grafana.

**Dashboard JSON shape note:** Provisioned files are the **dashboard document** (title/panels/uid at top level), not the HTTP API `{ "dashboard": { ... } }` wrapper. `[CITED: Grafana provisioning docs]`

### Pattern 2: ClickHouse Grafana datasource (HTTP)

**What:** Official plugin `type: grafana-clickhouse-datasource`, protocol HTTP to in-network host `clickhouse` port `8123`.  
**Why HTTP not Native:** Compose maps native as host `9002→9000`; inside Docker network native is `:9000`, but alert sink + healthchecks already standardize on HTTP `:8123`. HTTP avoids host-port confusion. `[CITED: plugin configure docs — HTTP 8123]`

```yaml
# Source: https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/configure/
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
      password: flinklab123   # align docker/.env; prefer ${CH_PASSWORD} if wired into grafana env
```

**Compose env (required):**

```yaml
environment:
  GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
```

Register plugin id + “install via GF_INSTALL_PLUGINS” in root README 版本矩阵 (ENG-01). First Grafana start needs network to download plugin — document in p03 README 踩坑.

**Fallback (discretion):** If plugin download fails offline, keep CH table + `verify` SQL for business metrics; Grafana JSON still ships with Prometheus-only panels + README “import after `grafana-cli plugins install …`”. Do not invent a second store.

### Pattern 3: Separate window metrics job (DataStream)

**What:** New main class consuming same Kafka topic, same WM strategy, tumbling event-time windows, CH sink.  
**When to use:** Always (D-02).  
**Why DataStream not Table/SQL:** p03 pom has no `flink-table-*` dependencies today; adding them expands shade/surface. e01 already proves `TumblingEventTimeWindows` + `AggregateFunction` for counts. `[VERIFIED: p03 pom + e01 HelloEventTimeWindowJob]`

**Suggested DDL columns (discretion — planner may tweak names):**

| Column | Type | Notes |
|--------|------|-------|
| `vin` | String | key |
| `window_start` / `window_end` | DateTime64(3) | tumbling bounds |
| `event_count` | UInt64 | all signals |
| `harsh_count` | UInt64 | `signalType='HARSH_ACCEL'` |
| `dtc_count` | UInt64 | `signalType='DTC'` |
| `ingest_time` | DateTime64(3) DEFAULT now64(3) | ops |

`p03-init`: third `wget --post-file` for CREATE (CH HTTP still no multi-query — keep separate file). `[VERIFIED: Phase 2 STATE — CREATE+ALTER split]`

**Submit:** extend Makefile with `submit-window` / `docker/Makefile submit-p03-window` using `-c …VehicleWindowMetricsJob`; same jar as alert job.

### Pattern 4: Anomaly = Grafana thresholds (not ML)

**What:** Panels with hard thresholds / Grafana alert rules documented in p03 docs.  
**Examples (demo defaults — replace with OrbStack-observed numbers in docs):**

| Panel | Query (sketch) | Threshold idea |
|-------|----------------|----------------|
| MATCH rate by `pattern_id` | CH: `countIf(alert_type='MATCH')` over `$__timeFilter` group by `pattern_id` | spike vs baseline row in `baseline.md` |
| Harsh per vin / window | CH: `harsh_count` from metrics table | e.g. demo default `> 3` in 1m window |
| Flink lag | Prom: `flink_taskmanager_job_task_operator_currentEmitEventTimeLag{job_name=~"p03.*"}` | rising during stall drill |

### Pattern 5: Watermark stall drill under `withIdleness(30s)`

**Critical fact:** `withIdleness(30s)` **excludes idle Kafka partitions from the watermark min** after 30s of silence — it does **not** advance time when **all** inputs are idle, and it does **not** help if a partition keeps receiving events with **stale eventTime**. `[CITED: Flink 2.2 Generating Watermarks — Dealing With Idle Sources]` + `[VERIFIED: docs/02-time-window + VehicleAlertJob]`

`vehicle.events` is created with **4 partitions**. Current gen uses a single `vin` key → typically one partition active; after 30s the other three go idle and stop holding WM. Therefore “leave unused partitions empty” alone is a **weak** stall demo once idleness fires.

**Recommended scripted drill (2 phases):**

1. **Stall (automated):**  
   - Produce a **partial** CEP sequence (e.g. HARSH without DTC) for a vin.  
   - Then produce a **steady trickle of HEARTBEAT with frozen `eventTime=T0`** (same key or multi-key) for ≥45s so the source is **not** marked idle, but watermark cannot advance past `T0 - ooo(5s)`.  
   - Assert: CH MATCH count unchanged; Prometheus `currentEmitEventTimeLag` for `Source:_kafka_vehicle_events` **increases** (wall clock moves, WM stuck).  
   - Manual/UI evidence: Flink UI → job → Timestamps/Watermarks column stagnant (document screenshot-or-checklist; REST `/vertices/{id}/watermarks` returned `[]` on live OrbStack this session — **do not rely on REST watermarks alone**). `[VERIFIED: live Flink REST returned empty watermarks array; Prom lag metric present]`

2. **Recover:**  
   - Emit completing DTC (or full scenario) + `_tail_heartbeats` with **advancing** timestamps; optionally emit heartbeats for keys hashing across partitions.  
   - Assert: CH MATCH ≥1 for expected `pattern_id`; lag drops or stops climbing.

**Optional teaching appendix (not required for exit 0):** pause all producers >30s to show “all idle ⇒ WM frozen” per docs/02 — checklist only.

### Pattern 6: Loadtest metric scrape

**What:** `loadtest.sh` wraps rate gen + Prom instant queries + CH counts → writes `docs/baseline.md`.  
**Verified Prom queries (live OrbStack):**

| Signal | PromQL (filter by `job_name` / `job_id`) |
|--------|------------------------------------------|
| Event-time lag | `flink_taskmanager_job_task_operator_currentEmitEventTimeLag{job_name=~"p03.*"}` |
| Checkpoint duration | `flink_jobmanager_job_lastCheckpointDuration{job_name=~"p03.*"}` |
| Restarts | `flink_jobmanager_job_numRestarts{job_name=~"p03.*"}` |
| Busy / backpressure | names in `monitoring/README.md` (underscore forms as exported) |

Scrape via:

```bash
curl -sG 'http://localhost:9090/api/v1/query' --data-urlencode 'query=...'
```

Throughput for p03 baseline: Kafka produce rate (configured `--rate`) + CH `vehicle_window_metrics` row growth and/or Flink `numRecordsInPerSecond` if present — declare formula in `baseline.md`. Warmup 30–60s discarded (D-08).

### Anti-Patterns to Avoid

- **Stuffing window agg into `VehicleAlertJob`:** violates D-02; risks CEP uid/savepoint story.
- **Anomaly via 4th CEP pattern or ML job:** violates D-04.
- **k6 image / full `benchmark/` matrix:** violates D-06/D-07 / deferred.
- **Screenshot-only Grafana proof:** violates D-01/D-03.
- **Relying on idle partitions alone for stall with idleness=30s:** stall disappears after idle timeout; use frozen-eventTime heartbeat instead.
- **Using Kafka alert topic as verify authority:** Phase 1–2 discipline; CH only for CEP.
- **Hard-binding `make up` to `--profile p03`:** forbidden.
- **Table API without SSOT/pom registration:** reject for this phase.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Dashboard persistence | Custom Grafana DB dumps | File provisioning + git JSON | Official GitOps path |
| CH Grafana SQL UI | Ad-hoc curl HTML | `grafana-clickhouse-datasource` | Query editor + provisioning |
| CH batch insert | New JDBC stack | Existing HTTP SinkV2 pattern | Already proven in p03/e07 |
| Load generator | k6/JMeter container | Extend `gen_vehicle_events.py` | D-07 + SSOT |
| Watermark observation | Custom JM hook | Prom lag + Flink UI checklist | REST watermarks empty in practice here |
| Anomaly detection engine | Custom CEP/ML | Grafana thresholds + docs | D-04 |

**Key insight:** Phase 3 is mostly **wiring and operational proof** on existing stack; complexity is compose/provisioning and drill design under idleness, not new frameworks.

## Common Pitfalls

### Pitfall 1: ClickHouse plugin missing → empty business panels
**What goes wrong:** Dashboard JSON references `uid: p03-clickhouse` but plugin not installed.  
**Why:** Base Grafana image has no ClickHouse plugin today (`/api/plugins` showed prometheus only).  
**How to avoid:** `GF_INSTALL_PLUGINS` + `verify_dashboard.sh` checks datasource type healthy via Grafana API.  
**Warning signs:** Panel errors “Datasource was not found”.

### Pitfall 2: Native port 9000 confusion
**What goes wrong:** Datasource points at `clickhouse:9000` from host or `localhost:9000` (MinIO collision on host).  
**How to avoid:** In-compose use `clickhouse:8123` HTTP; host tools use `localhost:8123`. `[VERIFIED: compose ports]`

### Pitfall 3: Idleness masks “idle partition” stall demos
**What goes wrong:** Drill waits on empty partitions; after 30s WM advances from the busy partition — “stall” never appears.  
**How to avoid:** Frozen-eventTime heartbeat pattern (Pattern 5). Teach idleness as recovery aid, not stall mechanism.

### Pitfall 4: Window job WM / consumer group fights alert job
**What goes wrong:** Two KafkaSource jobs with same `group.id` split partitions unexpectedly.  
**How to avoid:** Distinct `group.id` (e.g. `p03-window-metrics`) and independent offsets; document in ARCHITECTURE.

### Pitfall 5: p03-init multi-statement DDL
**What goes wrong:** Bundling CREATE metrics + alerts in one POST fails.  
**How to avoid:** Separate SQL file + separate wget POST (existing pattern).

### Pitfall 6: Unmeasured SLA numbers in threshold docs
**What goes wrong:** Writing “P99 < 100ms” without OrbStack run.  
**How to avoid:** D-05 — demo defaults labeled as such; baseline numbers only after `loadtest.sh`.

### Pitfall 7: Flink REST `/watermarks` empty
**What goes wrong:** Script asserts on empty JSON and flakes.  
**How to avoid:** Prefer Prometheus lag + CH MATCH; UI checklist as secondary evidence. `[VERIFIED: empty array on live job]`

## Code Examples

### Tumbling window count (align e01)

```java
// Source: examples/e01-hello-flink/.../HelloEventTimeWindowJob.java (in-repo pattern)
events
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((e, ts) -> e.eventTime)
            .withIdleness(Duration.ofSeconds(30)))
    .keyBy(e -> e.vin)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new EventCountAgg(), new AttachWindowMeta())
    .sinkTo(new ClickHouseWindowMetricsSink(baseUrl, user, password));
```

### Grafana dashboard health check (scriptable)

```bash
# Datasource list + dashboard search (admin/flinklab per compose)
curl -sf -u admin:flinklab http://localhost:3000/api/datasources \
  | grep -q grafana-clickhouse-datasource
curl -sf -u admin:flinklab 'http://localhost:3000/api/search?query=p03' \
  | grep -q p03-vehicle
test -s projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json
```

### Prometheus scrape for loadtest

```bash
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=flink_jobmanager_job_lastCheckpointDuration{job_name=~"p03.*"}'
```

### gen rate-mode sketch (extend existing)

```python
# Discretion: implement in gen_vehicle_events.py
# --rate 200 --duration 120 --vin-prefix VIN-LOAD
# emit HEARTBEAT/HARSH mix with advancing eventTime; optional --frozen-event-time for stall
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| AssignerWithPeriodicWatermarks | `WatermarkStrategy` + `withIdleness` | Flink 1.11+ / still current in 2.2 | Use strategy API only |
| Legacy Grafana simplejson hacks | Official ClickHouse plugin + file provisioning | Grafana 9–11 era | Provision YAML + plugin id |
| Screenshot dashboards | Git-versioned dashboard JSON | Ops GitOps norm | Required by D-01/D-03 |
| Commercial dynamic CEP | Precompiled Pattern + Broadcast gate | Project STACK / Phase 2 | ADR-0001 topic |

**Deprecated/outdated:**
- Relying solely on Flink UI for automation (no CI assert)
- Putting business KPIs only in Flink metrics without CH (conflicts with D-01 storage choice)

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `GF_INSTALL_PLUGINS=grafana-clickhouse-datasource` succeeds on OrbStack arm64 Grafana 11.1.0 without unsigned-plugin flags | Standard Stack | Need fallback Prometheus-only business panels or manual `grafana-cli` |
| A2 | Frozen-eventTime heartbeat reliably holds WM with current assignTimestampsAndWatermarks placement (post-parse, not on KafkaSource) | Pattern 5 | Drill must switch to all-idle stall + UI checklist only |
| A3 | Tumbling 1-minute windows are acceptable demo granularity for OrbStack short loadtests | Pattern 3 | May shorten to 10–30s windows if loadtest duration is short |
| A4 | Mounting p03 dashboard JSON into always-on Grafana is accepted reading of D-03 “not hard-bound” | Pattern 1 | If user wants zero p03 files in default Grafana, switch to API import script only |

## Open Questions

1. **Plugin install offline / first-boot network**
   - What we know: GF_INSTALL_PLUGINS needs download at start.
   - What's unclear: CI/sandbox without network.
   - Recommendation: Document; executor verifies on OrbStack; fallback path in README.

2. **Exact loadtest eps/duration**
   - What we know: CONTEXT leaves to executor.
   - Recommendation: start `100 eps × 120s`, warmup 45s; adjust until checkpoint/lag stable; write measured values only.

3. **Flink REST watermarks empty**
   - What we know: endpoint returned `[]` on RUNNING job.
   - Recommendation: do not block VEH-06 on REST watermarks; use Prom + CH + UI checklist.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker / OrbStack | All e2e | ✓ | Docker 29.4.0, Context orbstack | — |
| JDK 21 | package/submit | ✓ | 21.0.2 | — |
| Maven | package | ✓ | 3.9.14 | — |
| uv | gen/loadtest | ✓ | 0.10.10 | — |
| Grafana :3000 | VEH-05 | ✓ | 11.1.0 (live 200) | — |
| Prometheus :9090 | VEH-05/06 | ✓ | v2.53.0 (live 200) | — |
| ClickHouse :8123 | VEH-05/06/07 | ✓ | 24.8 healthy | — |
| Flink JM :8081 | drills | ✓ | job RUNNING | — |
| grafana-clickhouse-datasource | CH panels | ✗ (not installed) | — | Install via GF_INSTALL_PLUGINS; else Prom-only fallback |
| k6 | — | n/a | — | Out of scope (D-07) |
| Graphify knowledge graph | research enrichment | disabled | — | Skipped |

**Missing dependencies with no fallback:** none for planning (plugin install is an execution Wave-0/1 task).

**Missing dependencies with fallback:** ClickHouse Grafana plugin → Prometheus-only health + CH SQL verify for business metrics.

## Validation Architecture

> `workflow.nyquist_validation` is **true** in `.planning/config.json` — this section is required for VALIDATION.md extraction.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 5.10.2 + Maven Surefire 3.2.5 (existing p03) |
| Config file | `projects/p03-vehicle-monitoring/pom.xml` surefire plugin |
| Quick run command | `cd projects/p03-vehicle-monitoring && mvn -q test` |
| Full suite command | `mvn -q test` + `bash scripts/verify.sh` + dashboard/loadtest/drill scripts on OrbStack |
| Gate script | `scripts/qa_check.sh` (compose config, 违禁词, links) at phase end |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VEH-05 | Window metrics rows land in CH | e2e / smoke | After submit-window + gen: CH `count() FROM vehicle_window_metrics >= 1` | ❌ Wave 0 — add assert to `verify_dashboard.sh` or `verify_metrics.sh` |
| VEH-05 | Dashboard JSON present + Grafana sees it | smoke | `test -s monitoring/dashboards/*.json` + Grafana `/api/search` | ❌ Wave 0 |
| VEH-05 | ClickHouse + Prometheus datasources healthy | smoke | Grafana `/api/datasources` | ❌ Wave 0 |
| VEH-05 | Threshold rules documented | doc gate | File exists + non-empty thresholds section | ❌ Wave 0 (docs) |
| VEH-06 | `loadtest.sh` fails non-zero on Prom/CH unreachable | script | `bash -n` + negative path | ❌ Wave 0 |
| VEH-06 | `drill_watermark_stall.sh` stall→recover | e2e OrbStack | script exit 0; MATCH appears after recover | ❌ Wave 0 |
| VEH-06 | `docs/baseline.md` has measured table | artifact | grep for table + env snapshot headings | ❌ Wave 0 |
| VEH-07 | ADR path openable | doc | `test -f docs/adr/0001-cep-broadcast-precompiled.md` | ❌ Wave 0 |
| VEH-07 | RESUME + ARCHITECTURE | doc | path checks | ❌ Wave 0 |
| VEH-07 | CEP verify still CH authority | regression | `PATTERN_ID=… bash scripts/verify.sh` | ✅ `scripts/verify.sh` |
| ENG | Window agg unit (count accumulator) | unit | `mvn -q test` new `EventCountAggTest` | ❌ Wave 0 |
| ENG | No TODO/省略 in new docs | qa | `scripts/qa_check.sh` | ✅ scripts/qa_check.sh |

### Sampling Rate

- **Per task commit:** `mvn -q test` in p03 module  
- **Per wave merge:** unit tests + `bash scripts/verify.sh` (CEP regression)  
- **Phase gate:** `verify.sh` + `verify_dashboard.sh` + `loadtest.sh` (or shorter smoke) + `drill_watermark_stall.sh` + `qa_check.sh`; OrbStack evidence required before claiming VEH-05/06 done  

### Wave 0 Gaps

- [ ] `scripts/verify_dashboard.sh` — dashboard file + Grafana API datasource/dashboard checks (VEH-05/D-15)
- [ ] `scripts/loadtest.sh` + `scripts/drill_watermark_stall.sh` — fail non-zero skeletons (VEH-06)
- [ ] Unit test for window `AggregateFunction` accumulator (optional but cheap Nyquist RED→GREEN)
- [ ] `sql/clickhouse_window_metrics.sql` + p03-init third POST
- [ ] Grafana `provisioning/dashboards/dashboards.yml` + ClickHouse datasource YAML + `GF_INSTALL_PLUGINS`
- [ ] Framework install: none — JUnit/Surefire already present

### OrbStack command sketch (phase gate)

```bash
cd docker && make up && make up-p03
cd ../projects/p03-vehicle-monitoring
make package && make submit && make submit-window   # names per planner
make gen && bash scripts/verify.sh
bash scripts/verify_dashboard.sh
bash scripts/drill_watermark_stall.sh
bash scripts/loadtest.sh                            # writes docs/baseline.md
cd ../.. && bash scripts/qa_check.sh
```

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | partial | Grafana admin basic auth (existing lab default); do not expose anonymously |
| V3 Session Management | no | N/A for batch drills |
| V4 Access Control | partial | Prefer read-only CH user for Grafana DS in docs; lab may keep `flinklab` with note |
| V5 Input Validation | yes | Continue whitelist `pattern_id` / reject quotes in sinks; gen JSON schema discipline |
| V6 Cryptography | no | No new crypto; plaintext lab passwords already in `.env` |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SQL injection via alert/metrics sink | Tampering | Whitelist enums; escape/reject `'` `\`; constant table/column names (existing ClickHouseAlertSink) |
| Grafana panel SQL abuse | Elevation | Document read-only CH user; plugin warns queries execute as configured user `[CITED: plugin docs]` |
| Credential leakage in provisioned YAML | Info disclosure | Prefer env substitution `${CH_PASSWORD}`; do not commit production secrets |
| Dashboard JSON XSS | Tampering | Provision from git only; `allowUiUpdates: false` |
| Loadtest accidental prod Kafka | DoS | Hardcode localhost:9094 defaults; no cloud endpoints |

## Sources

### Primary (HIGH confidence)

- Grafana provisioning (dashboards providers) — https://grafana.com/docs/grafana/latest/administration/provisioning/#dashboards
- Grafana ClickHouse datasource configure/provision — https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/configure/
- Flink 2.2 Generating Watermarks / idleness — https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/generating_watermarks/
- In-repo: `VehicleAlertJob`, `ClickHouseAlertSink`, `gen_vehicle_events.py`, `verify.sh`, `docker/docker-compose.yml`, `monitoring/README.md`, `benchmark/README.md`, `docs/02-time-window/README.md`, e01 window jobs
- Live OrbStack probes: Grafana/Prom/Flink/CH HTTP 200; Prom metric names; datasource inventory; topic partitions=4

### Secondary (MEDIUM confidence)

- ClickHouse Grafana integration docs — https://clickhouse.com/docs/integrations/grafana/config
- Community note on `host` vs `server` provisioning key drift — treat official `host` as canonical; verify via Grafana `/api/datasources` after first boot

### Tertiary (LOW confidence)

- Flink REST `/watermarks` emptiness root cause on this cluster — not fully diagnosed; mitigated by Prom lag

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — versions and live services verified; plugin install path cited from official docs (arm64 download still A1)
- Architecture: HIGH — locked CONTEXT + existing p03 patterns; window job / dual DS clear
- Pitfalls: HIGH — idleness vs stall and plugin gap verified against code + live cluster

**Research date:** 2026-07-18  
**Valid until:** 2026-08-18 (30 days; Grafana plugin / Flink REST quirks may shift sooner)
