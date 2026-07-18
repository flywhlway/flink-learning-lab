# p03 · 车联网监控告警链路样板

> 对应教材:[docs/10-cep](../../docs/10-cep/README.md) · 企业实战模块 15 · GSD Phase 1–3（VEH-01–VEH-06）
> 模式五元组评审见 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md)。Grafana 双 DS 大盘、压测 baseline 与 watermark 演练见下文；ADR/简历页属同里程碑后续 plan。

## 1. 背景

车联网监控要把「激烈驾驶后短时间内出现故障码」以及「连续急加速 / 重复故障码」从海量遥测里稳定抓出来。窗口聚合擅长计数，SQL 擅长关联，但**跨事件时序 + 超时半成品**更适合 CEP（NFA）。本项目在 e10-C5 雏形与 e10-C3 Side Output 之上，落成独立 compose profile `p03` 的生产骨架：事件进 Kafka → 静态三路 CEP → Broadcast 出口门控 → 告警双写 ClickHouse / Kafka，并用 `PATTERN_ID` 断言验收。

硬验收：

- 独立 `make up-p03` 不影响 default `make up`
- 未发控制消息时默认激活 `HARSH_THEN_FAULT`：造数 `match-harsh-fault` 后 CH 出现该 `pattern_id` 的 MATCH（D-06）
- 发布控制消息切换激活集后，造数命中目标模式，CH 按 `pattern_id` 断言变化（D-10）；**不必重启作业换参数**
- `verify.sh` **以 ClickHouse 为唯一权威出口**；Kafka 仅诊断；失败非 0
- 旁路窗口作业写入 `vehicle_window_metrics`；Grafana 打开 `p03-vehicle-overview`；`make verify-dashboard` exit 0（VEH-05）

## 2. 架构

```text
[gen_vehicle_events.py] --produce--> Kafka vehicle.events (宿主机 localhost:9094)
[gen --publish-control] -----------> Kafka vehicle.pattern.control
                                            |
                               Flink JobManager / TaskManager
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
        VehicleAlertJob（CEP 主图，勿改）              VehicleWindowMetricsJob（旁路）
        WM: ooo=5s, idleness=30s                      同 WM；groupId=p03-window-metrics
        三 CEP → Gate → 双写                          keyBy(vin)→30s tumbling→CH Sink
                     │                                             │
                     ▼                                             ▼
        CH vehicle_alerts (+ Kafka alerts)            CH vehicle_window_metrics
                     │                                             │
                     └──────────────────┬──────────────────────────┘
                                        ▼
                     Grafana p03-vehicle-overview（双 DS）
              ClickHouse 业务面板 │ Prometheus Flink 健康
```

开源 Flink CEP 的 Pattern 在编译期固定；运行期动态化路线是 **静态三 CEP + Broadcast 选择预编译激活集**，不引入商业动态规则引擎、不在运行时编译 Pattern。详见 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md)。窗口聚合与 CEP **解耦**（D-02）：大盘吞吐走旁路作业，不污染 Gate 语义。

Compose 隔离：`profiles: ["p03"]` 的 `p03-init` 幂等创建 topic + `vehicle_alerts` / `vehicle_window_metrics` DDL；不写入 default `init.sh`。Grafana 大盘 JSON 挂载不绑 `--profile p03`。

## 3. 代码

| 路径 | 职责 |
|---|---|
| `VehicleAlertJob` | 三 CEP + control Broadcast + 门控 + 双写；算子均带 `.uid(...)` |
| `VehicleWindowMetricsJob` | 旁路窗口作业；默认 30s tumbling；uid 前缀 `p03-wm-` |
| `window/EventCountAgg` + `AttachWindowMeta` | event/harsh/dtc 三分计数 + 窗口元信息 |
| `ClickHouseWindowMetricsSink` | HTTP SinkV2 → `flinklab.vehicle_window_metrics` |
| `HarshThenFaultPattern` / `TripleHarshPattern` / `DtcPairPattern` | 预编译模式，均强制 `within` |
| `PatternRegistry` / `PatternIds` | 恰好三常量注册表 |
| `PatternActivationGate` | Broadcast State 按 `activePatterns`∩白名单过滤出口 |
| `ParseVehicleJson` / `ParsePatternControlJson` | 事件/控制 JSON；脏数据丢弃 |
| `ClickHouseAlertSink` | HTTP SinkV2 → `flinklab.vehicle_alerts`（含 `pattern_id`） |
| `monitoring/dashboards/p03-vehicle-overview.json` | Grafana 双 DS 大盘（可 provisioning） |
| `scripts/gen_vehicle_events.py` | 三 scenario + `--rate/--duration` + `--frozen-event-time` + `--publish-control` |
| `scripts/verify.sh` | `PATTERN_ID` 白名单 + CH MATCH 权威断言 |
| `scripts/verify_dashboard.sh` | 大盘 JSON + Grafana API + CH metrics smoke |
| `scripts/loadtest.sh` | 项目级压测 → [`docs/baseline.md`](docs/baseline.md)（Prometheus + CH） |
| `scripts/drill_watermark_stall.sh` | 冻结 eventTime HEARTBEAT 停滞 → 恢复后 MATCH 断言 |
| `docs/PATTERN-LIBRARY.md` | 三模式五元组评审页 |
| `docs/ANOMALY-THRESHOLDS.md` | 异常阈值条文（演示默认 / 非生产 SLA） |
| `docs/baseline.md` | OrbStack arm64 实测压测数字（由 `make loadtest` 生成） |

Maven 独立模块（不挂 `examples/` 父工程），版本对齐仓库 SSOT（Flink 2.2.1 / Kafka connector 5.0.0-2.2）。

## 4. 启动

前置：OrbStack arm64，本机已安装 JDK 21、Maven、uv、Docker。

```bash
# 1) 基座（不含 p03 profile）
cd docker
make up
make init

# 2) p03 topic（含 vehicle.pattern.control）+ ClickHouse DDL
make up-p03

# 3) 打包并提交作业（告警 + 旁路窗口）
cd ../projects/p03-vehicle-monitoring
make package
make submit
make submit-window
# 等价：cd docker && make submit-p03 && make submit-p03-window

# 4) 默认造数（match 别名 → match-harsh-fault；未发 control）
make gen
```

端口速查：Flink UI `http://localhost:8081`；Kafka 宿主机 `localhost:9094`（容器内作业用 `kafka:9092`，勿混用）；ClickHouse `localhost:8123`（`flinklab` / `flinklab123`，与 `docker/.env` 一致，仅本地 lab）；Grafana `http://localhost:3000`（`admin` / `flinklab`）。

## 5. 验证

权威出口是 ClickHouse，不是 Kafka。

### 5.1 默认回归（D-06）

```bash
cd projects/p03-vehicle-monitoring
make truncate-alerts   # 可选：清旧行
make gen
# 轮询至绿（作业落库约数秒～数十秒）
PATTERN_ID=HARSH_THEN_FAULT make verify
# 内部：SELECT count() FROM flinklab.vehicle_alerts
#   WHERE alert_type='MATCH' AND pattern_id='HARSH_THEN_FAULT'
# count≥1 → exit 0；否则 exit 1（即使 Kafka vehicle.alerts 仍有历史消息）
```

### 5.2 Broadcast 切换剧本（D-10）

主路径是发布控制 JSON，**不是**重启作业换参数：

```bash
cd projects/p03-vehicle-monitoring
# 串联：TRUNCATE → --publish-control → match-triple-harsh → PATTERN_ID=TRIPLE_HARSH verify
make verify-switch
# 复跑时 version 须单调递增：make verify-switch CONTROL_VERSION=3
```

等价手工步骤：

```bash
make truncate-alerts
uv run scripts/gen_vehicle_events.py \
  --publish-control '{"activePatterns":["TRIPLE_HARSH"],"version":2}'
uv run scripts/gen_vehicle_events.py --scenario match-triple-harsh
PATTERN_ID=TRIPLE_HARSH bash scripts/verify.sh
```

控制面契约（D-04）：topic `vehicle.pattern.control`，JSON `{"activePatterns":["..."],"version":N}`；`version` 单调更新才生效。造数场景：`match-harsh-fault` / `match-triple-harsh` / `match-dtc-pair`（`match` 为 Phase 1 别名）。

目视：

```bash
cd docker
docker compose exec -T clickhouse clickhouse-client \
  --user flinklab --password flinklab123 \
  --query "SELECT vin, alert_type, pattern_id, harsh_value, fault_value, event_time \
           FROM flinklab.vehicle_alerts ORDER BY ingest_time DESC LIMIT 5"
```

负例（证明 CH 权威）：

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user flinklab --password flinklab123 \
  --query "TRUNCATE TABLE flinklab.vehicle_alerts"
cd ../projects/p03-vehicle-monitoring && make verify   # 必须非 0；不必清 Kafka
```

TIMEOUT Side Output 契约由单测覆盖；e2e 不强制造超时半成品。端到端采样（package→submit→gen→wait→verify / verify-switch）预算上限 300 秒。

### 5.3 Grafana 大盘与窗口指标（VEH-05）

窗口作业默认 **30 秒** event-time tumbling（短于 1 分钟，便于 OrbStack 快速落库；与 `VehicleWindowMetricsJob.WINDOW_SIZE` 一致）。

```bash
cd projects/p03-vehicle-monitoring
make submit-window          # 若尚未提交
make gen                    # 推进 watermark 后窗口关闭
# 目视 CH
docker compose -f ../../docker/docker-compose.yml exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "SELECT vin, window_start, event_count, harsh_count, dtc_count \
           FROM flinklab.vehicle_window_metrics ORDER BY ingest_time DESC LIMIT 5"

# 大盘门禁（JSON 非空 + ClickHouse DS 插件 + /api/search 命中 p03-vehicle + metrics count≥1）
make verify-dashboard
```

浏览器打开：

1. [http://localhost:3000](http://localhost:3000)（`admin` / `flinklab`）
2. 文件夹 **p03** → 大盘 **p03-vehicle-overview**  
   或直达 [http://localhost:3000/d/p03-vehicle-overview/p03-vehicle-overview](http://localhost:3000/d/p03-vehicle-overview/p03-vehicle-overview)

面板覆盖：窗口吞吐（CH）、按 `pattern_id` 的 MATCH 速率（CH）、异常阈值 stat（CH）、Flink checkpoint/重启/event-time lag/反压（Prometheus）。阈值条文见 [docs/ANOMALY-THRESHOLDS.md](docs/ANOMALY-THRESHOLDS.md)。可导入 JSON 路径：`monitoring/dashboards/p03-vehicle-overview.json`。

### 5.4 压测与 watermark 演练（VEH-06）

两条 MVP 演练入口（失败非 0；无 k6/JMeter）：

```bash
cd projects/p03-vehicle-monitoring

# (1) 项目级压测：热身丢弃 → rate×duration → 刮 Prometheus/CH → 写 docs/baseline.md
make loadtest
# 覆盖参数示例：RATE=100 WARMUP_SEC=45 DURATION_SEC=120 make loadtest

# (2) watermark 停滞：部分 HARSH + 冻结 eventTime HEARTBEAT ≥45s → 恢复后 PATTERN_ID verify
make drill-watermark
```

预期：

- `make loadtest` 生成 [`docs/baseline.md`](docs/baseline.md)（环境 / 负载 / 指标 / 结论），数字来自本次 OrbStack 运行。
- `make drill-watermark` stall 阶段 CH MATCH 不增且 Prometheus `currentEmitEventTimeLag` 上升；recover 后 `PATTERN_ID=HARSH_THEN_FAULT` 的 `verify.sh` 为绿。
- 副证（human-check）：stall 期间 Flink UI → 作业 → Timestamps/Watermarks 列停滞（REST `/watermarks` 可能为空，勿作唯一断言）。时间语义见 [docs/02-time-window](../../docs/02-time-window/README.md)。

造数速率模式：`uv run scripts/gen_vehicle_events.py --rate 100 --duration 60`（`--eps` 为别名）；冻结心跳：`--frozen-event-time --duration 50 --rate 2 --vin VIN-STALL-001`。

## 6. 踩坑

1. **bootstrap 混用**：宿主机造数/控制必须 `localhost:9094`；作业默认 `kafka:9092`。写反则「topic 有数据、作业不消费」或反之。
2. **Pattern 无 `within`**：部分匹配永不释放，状态膨胀；`PatternRegistryWithinTest` 强制三模式均有 `within`。
3. **watermark 停滞**：多分区时空闲分区会卡住水位；作业配置 `withIdleness(30s)` 会在 30s 后排除空闲分区——因此演练**不能只靠空闲分区**，须用冻结 `eventTime` 的 HEARTBEAT 涓流（`make drill-watermark`）。造数恢复时须追加足够晚的 HEARTBEAT，把水位推过末事件（BoundedOutOfOrderness=5s），否则 MATCH 迟迟不落库。
4. **`TRIPLE_HARSH` 被 HEARTBEAT 打断**：`times(3).consecutive()` 中间夹其它事件即作废；`match-triple-harsh` 故意不在三次 HARSH 之间插心跳。
5. **门控关闭 ≠ CEP 状态停止**：Broadcast 仅过滤出口；三路 CEP 仍占状态，靠各自 `within` TTL 回收（见 PATTERN-LIBRARY）。
6. **切换后不 TRUNCATE**：旧 `pattern_id` 行会污染断言；`verify-switch` 先清表。
7. **ClickHouse Sink flush / DDL**：表缺 `pattern_id` 时 Sink INSERT 失败——先 `make up-p03`（CREATE 与 ALTER 分 POST）。
8. **MinIO / S3a 凭证链**：须使用 `SimpleAWSCredentialsProvider`，否则作业长时间 `INITIALIZING`。
9. **端口冲突**：本机其他 MinIO 占用 `9000` 时 JM 依赖 `minio-init` 会失败。
10. **Grafana ClickHouse 插件未装**：首次 `GF_INSTALL_PLUGINS=grafana-clickhouse-datasource` 需联网下载；`verify_dashboard` 会对 `grafana-clickhouse-datasource` / `p03-clickhouse` 明确 FAIL。修复：`cd docker && docker compose up -d --force-recreate grafana`；离线可 `docker compose exec grafana grafana-cli plugins install grafana-clickhouse-datasource` 后 `restart grafana`。禁止用截图代替 JSON。
11. **窗口指标为空**：须独立 `make submit-window`（groupId=`p03-window-metrics`，勿与告警作业共享）；造数后等待水位推过窗口结束（30s + ooo=5s + 尾心跳）。

## 7. 最佳实践

- 每条 CEP 模式在 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md) 登记五元组：业务含义 / within / 连接语义 / skip 策略 / 状态上界；无 `within` 不得合入。
- 验证脚本对 `PATTERN_ID` 白名单后再拼 SQL；Kafka 消费只做诊断日志。
- 控制面用 Kafka 确定性 JSON + version 单调；禁止本地文件热读或重启换模式作为验收主路径。
- 有意义算子一律显式 `.uid(...)`，为后续 Savepoint 升级留契约。
- MATCH 与 TIMEOUT 同表同 topic，用 `alert_type` + `pattern_id` 区分。
- profile 隔离：业务 init 进 `p03`，永不污染 default `make up`。
- 大盘业务指标走 CH 表 + Grafana ClickHouse DS；平台健康走 Prometheus；异常用阈值面板 + [ANOMALY-THRESHOLDS](docs/ANOMALY-THRESHOLDS.md)，不扩 CEP 冒充异常检测。

## 8. 面试题与参考

**面试题**

1. CEP 的 `within` 超时由什么时钟驱动？watermark 停滞时会出现什么现象？
2. 开源 Flink CEP 如何在不重启作业的前提下「切换模式」？门控关闭后未激活模式的状态还在增长吗？
3. 为什么本项目的 `verify.sh` 不能用「Kafka 有消息」单独放行？
4. `times(3).consecutive()` 与 `followedBy` 在穿插 `HEARTBEAT` 时行为有何不同？
5. 为什么窗口聚合要做成旁路作业而不是塞进 `VehicleAlertJob`？大盘为何用双数据源？

**参考**

- 本项目模式库 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md)
- 异常阈值 [docs/ANOMALY-THRESHOLDS.md](docs/ANOMALY-THRESHOLDS.md)；大盘 JSON `monitoring/dashboards/p03-vehicle-overview.json`
- 教材 [docs/10-cep](../../docs/10-cep/README.md)；时间窗口 [docs/02-time-window](../../docs/02-time-window/README.md)；值班指标 [monitoring/README.md](../../monitoring/README.md)
- 案例 [examples/e10-cep](../../examples/e10-cep/README.md)（C1/C3/C5）；e01 窗口聚合
- Flink 2.2 CEP Pattern API / Broadcast State / TimedOutPartialMatchHandler（官方文档）
- 本仓库 `docker/Makefile`（`up-p03` / `submit-p03` / `submit-p03-window`）、`sql/clickhouse_window_metrics.sql`
- ADR / 简历陈述页延同里程碑 VEH-07 plan；模块 15 登记见 [docs/README.md](../../docs/README.md)
