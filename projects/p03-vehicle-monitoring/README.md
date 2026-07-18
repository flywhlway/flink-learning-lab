# p03 · 车联网监控告警链路样板

> 对应教材:[docs/10-cep](../../docs/10-cep/README.md) · 企业实战模块 15 · GSD Phase 1–2（VEH-01–VEH-04）
> 模式五元组评审见 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md)。完整 ADR / Grafana 大盘 / 压测属后续 Phase。

## 1. 背景

车联网监控要把「激烈驾驶后短时间内出现故障码」以及「连续急加速 / 重复故障码」从海量遥测里稳定抓出来。窗口聚合擅长计数，SQL 擅长关联，但**跨事件时序 + 超时半成品**更适合 CEP（NFA）。本项目在 e10-C5 雏形与 e10-C3 Side Output 之上，落成独立 compose profile `p03` 的生产骨架：事件进 Kafka → 静态三路 CEP → Broadcast 出口门控 → 告警双写 ClickHouse / Kafka，并用 `PATTERN_ID` 断言验收。

硬验收：

- 独立 `make up-p03` 不影响 default `make up`
- 未发控制消息时默认激活 `HARSH_THEN_FAULT`：造数 `match-harsh-fault` 后 CH 出现该 `pattern_id` 的 MATCH（D-06）
- 发布控制消息切换激活集后，造数命中目标模式，CH 按 `pattern_id` 断言变化（D-10）；**不必重启作业换参数**
- `verify.sh` **以 ClickHouse 为唯一权威出口**；Kafka 仅诊断；失败非 0

## 2. 架构

```text
[gen_vehicle_events.py] --produce--> Kafka vehicle.events (宿主机 localhost:9094)
[gen --publish-control] -----------> Kafka vehicle.pattern.control
                                            |
                               Flink JobManager / TaskManager
                                            |
              KafkaSource(events) → ParseVehicleJson → Watermark(ooo=5s,idleness=30s)
                                            |
                                   keyBy(vin) → 静态三 CEP（并行挂载）
                    HARSH_THEN_FAULT | TRIPLE_HARSH | DTC_PAIR（均 within）
                           MATCH / TIMEOUT → union(AlertEvent+patternId)
                                            |
              KafkaSource(control) → ParsePatternControlJson → broadcast
                                            |
                         allAlerts.connect(controlBroadcast) → PatternActivationGate
                              （门控≠停 CEP 状态：未激活模式 NFA 仍跑，仅抑制输出）
                                        /              \
                         Kafka vehicle.alerts     ClickHouse vehicle_alerts.pattern_id
                                        \
                               [verify.sh] PATTERN_ID 断言 CH MATCH count≥1
```

开源 Flink CEP 的 Pattern 在编译期固定；运行期动态化路线是 **静态三 CEP + Broadcast 选择预编译激活集**，不引入商业动态规则引擎、不在运行时编译 Pattern。详见 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md)。

Compose 隔离：`profiles: ["p03"]` 的 `p03-init` 幂等创建 `vehicle.events` / `vehicle.alerts` / `vehicle.pattern.control` + DDL；不写入 default `init.sh`。

## 3. 代码

| 路径 | 职责 |
|---|---|
| `VehicleAlertJob` | 三 CEP + control Broadcast + 门控 + 双写；算子均带 `.uid(...)` |
| `HarshThenFaultPattern` / `TripleHarshPattern` / `DtcPairPattern` | 预编译模式，均强制 `within` |
| `PatternRegistry` / `PatternIds` | 恰好三常量注册表 |
| `PatternActivationGate` | Broadcast State 按 `activePatterns`∩白名单过滤出口 |
| `ParseVehicleJson` / `ParsePatternControlJson` | 事件/控制 JSON；脏数据丢弃 |
| `ClickHouseAlertSink` | HTTP SinkV2 → `flinklab.vehicle_alerts`（含 `pattern_id`） |
| `scripts/gen_vehicle_events.py` | 三 scenario + `--publish-control`；尾心跳推进 watermark |
| `scripts/verify.sh` | `PATTERN_ID` 白名单 + CH MATCH 权威断言 |
| `docs/PATTERN-LIBRARY.md` | 三模式五元组评审页 |

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

# 3) 打包并提交作业
cd ../projects/p03-vehicle-monitoring
make package
make submit
# 等价：cd docker && make submit-p03

# 4) 默认造数（match 别名 → match-harsh-fault；未发 control）
make gen
```

端口速查：Flink UI `http://localhost:8081`；Kafka 宿主机 `localhost:9094`（容器内作业用 `kafka:9092`，勿混用）；ClickHouse `localhost:8123`（`flinklab` / `flinklab123`，与 `docker/.env` 一致，仅本地 lab）。

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

## 6. 踩坑

1. **bootstrap 混用**：宿主机造数/控制必须 `localhost:9094`；作业默认 `kafka:9092`。写反则「topic 有数据、作业不消费」或反之。
2. **Pattern 无 `within`**：部分匹配永不释放，状态膨胀；`PatternRegistryWithinTest` 强制三模式均有 `within`。
3. **watermark 停滞**：多分区时空闲分区会卡住水位；作业配置 `withIdleness(30s)`。造数须在业务末事件后追加足够晚的 HEARTBEAT，把水位推过末事件（BoundedOutOfOrderness=5s），否则 MATCH 迟迟不落库。
4. **`TRIPLE_HARSH` 被 HEARTBEAT 打断**：`times(3).consecutive()` 中间夹其它事件即作废；`match-triple-harsh` 故意不在三次 HARSH 之间插心跳。
5. **门控关闭 ≠ CEP 状态停止**：Broadcast 仅过滤出口；三路 CEP 仍占状态，靠各自 `within` TTL 回收（见 PATTERN-LIBRARY）。
6. **切换后不 TRUNCATE**：旧 `pattern_id` 行会污染断言；`verify-switch` 先清表。
7. **ClickHouse Sink flush / DDL**：表缺 `pattern_id` 时 Sink INSERT 失败——先 `make up-p03`（CREATE 与 ALTER 分 POST）。
8. **MinIO / S3a 凭证链**：须使用 `SimpleAWSCredentialsProvider`，否则作业长时间 `INITIALIZING`。
9. **端口冲突**：本机其他 MinIO 占用 `9000` 时 JM 依赖 `minio-init` 会失败。

## 7. 最佳实践

- 每条 CEP 模式在 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md) 登记五元组：业务含义 / within / 连接语义 / skip 策略 / 状态上界；无 `within` 不得合入。
- 验证脚本对 `PATTERN_ID` 白名单后再拼 SQL；Kafka 消费只做诊断日志。
- 控制面用 Kafka 确定性 JSON + version 单调；禁止本地文件热读或重启换模式作为验收主路径。
- 有意义算子一律显式 `.uid(...)`，为后续 Savepoint 升级留契约。
- MATCH 与 TIMEOUT 同表同 topic，用 `alert_type` + `pattern_id` 区分。
- profile 隔离：业务 init 进 `p03`，永不污染 default `make up`。

## 8. 面试题与参考

**面试题**

1. CEP 的 `within` 超时由什么时钟驱动？watermark 停滞时会出现什么现象？
2. 开源 Flink CEP 如何在不重启作业的前提下「切换模式」？门控关闭后未激活模式的状态还在增长吗？
3. 为什么本项目的 `verify.sh` 不能用「Kafka 有消息」单独放行？
4. `times(3).consecutive()` 与 `followedBy` 在穿插 `HEARTBEAT` 时行为有何不同？

**参考**

- 本项目模式库 [docs/PATTERN-LIBRARY.md](docs/PATTERN-LIBRARY.md)
- 教材 [docs/10-cep](../../docs/10-cep/README.md)；案例 [examples/e10-cep](../../examples/e10-cep/README.md)（C1/C3/C5）
- Flink 2.2 CEP Pattern API / Broadcast State / TimedOutPartialMatchHandler（官方文档）
- 本仓库 `docker/Makefile`（`up-p03` / `submit-p03`）、`sql/clickhouse_alerts.sql`
- ADR 完整内容延 Phase 3（VEH-07）；模块 15 登记见 [docs/README.md](../../docs/README.md)
