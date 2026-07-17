# p03 · 车联网监控告警链路样板

> 对应教材:[docs/10-cep](../../docs/10-cep/README.md) · 企业实战模块 15 · GSD Phase 1（VEH-01/VEH-02）
> 完整 ADR / 简历页 / Grafana 大盘 / Broadcast 模式库属后续 Phase，本目录交付可复现告警半段。

## 1. 背景

车联网监控要把「激烈驾驶后短时间内出现故障码」从海量遥测里稳定抓出来。窗口聚合擅长计数，SQL 擅长关联，但**跨事件时序 + 超时半成品**更适合 CEP（NFA）。本项目在 e10-C5 雏形与 e10-C3 Side Output 之上，落成独立 compose profile `p03` 的生产骨架：事件进 Kafka → Flink CEP → 告警双写 ClickHouse / Kafka，并用断言脚本验收。

Phase 1 硬验收：

- 独立 `make up-p03` 不影响 default `make up`
- 造数注入可判定 `HARSH_ACCEL → DTC` 后，ClickHouse `flinklab.vehicle_alerts` 出现 `MATCH`
- `verify.sh` **以 ClickHouse 为唯一权威出口**；Kafka 仅诊断；失败非 0

## 2. 架构

```text
[gen_vehicle_events.py] --produce--> Kafka vehicle.events (宿主机 localhost:9094)
                                            |
                               Flink JobManager / TaskManager
                                            |
              KafkaSource → ParseVehicleJson → Watermark(ooo=5s,idleness=30s)
                                            |
                                   keyBy(vin) → CEP
                         HARSH_ACCEL(>450) followedBy DTC(>480) within(30s)
                           /                                      \
                    processMatch                            processTimedOutMatch
                     alert_type=MATCH                        Side Output TIMEOUT
                           \                                      /
                            \__________ union(AlertEvent) _______/
                                        /              \
                         Kafka vehicle.alerts     ClickHouse vehicle_alerts
                                        \
                               [verify.sh] 仅断言 CH MATCH count≥1
```

Compose 隔离：`profiles: ["p03"]` 的 `p03-init` 幂等创建 topic + DDL；不写入 default `init.sh`。

## 3. 代码

| 路径 | 职责 |
|---|---|
| `VehicleAlertJob` | 管线装配：Source / WM / CEP / 双写 Sink；算子均带 `.uid(...)` |
| `HarshThenFaultPattern` | `followedBy` + `within(30s)` |
| `AlertPatternHandler` | MATCH 主流 + TIMEOUT Side Output |
| `ParseVehicleJson` | 白名单 `HARSH_ACCEL\|DTC\|HEARTBEAT`；脏数据丢弃 |
| `ClickHouseAlertSink` | HTTP SinkV2 → `flinklab.vehicle_alerts` |
| `scripts/gen_vehicle_events.py` | `--scenario match` 可判定序列 + 尾心跳推进 watermark |
| `scripts/verify.sh` | CH `alert_type='MATCH'` count 权威断言 |

Maven 独立模块（不挂 `examples/` 父工程），版本对齐仓库 SSOT（Flink 2.2.1 / Kafka connector 5.0.0-2.2）。

## 4. 启动

前置：OrbStack arm64，本机已安装 JDK 21、Maven、uv、Docker。

```bash
# 1) 基座（不含 p03 profile）
cd docker
make up
make init

# 2) p03 topic + ClickHouse DDL
make up-p03

# 3) 打包并提交作业
cd ../projects/p03-vehicle-monitoring
make package
make submit
# 等价：cd docker && make submit-p03

# 4) 造数
make gen
```

端口速查：Flink UI `http://localhost:8081`；Kafka 宿主机 `localhost:9094`（容器内作业用 `kafka:9092`，勿混用）；ClickHouse `localhost:8123`（`flinklab` / `flinklab123`，与 `docker/.env` 一致）。

## 5. 验证

权威出口是 ClickHouse，不是 Kafka：

```bash
cd projects/p03-vehicle-monitoring
make verify
# 内部：SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH'
# count≥1 → exit 0；否则 exit 1（即使 Kafka vehicle.alerts 仍有历史消息）
```

目视：

```bash
cd docker
docker compose exec -T clickhouse clickhouse-client \
  --user flinklab --password flinklab123 \
  --query "SELECT vin, alert_type, harsh_value, fault_value, event_time \
           FROM flinklab.vehicle_alerts ORDER BY ingest_time DESC LIMIT 5"
```

负例（证明 CH 权威）：

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user flinklab --password flinklab123 \
  --query "TRUNCATE TABLE flinklab.vehicle_alerts"
cd ../projects/p03-vehicle-monitoring && make verify   # 必须非 0；不必清 Kafka
```

TIMEOUT Side Output 契约由 `HarshThenFaultPatternTest` 单测覆盖；e2e 不强制造超时半成品。端到端采样（package→submit→gen→wait→verify）预算上限 300 秒。

## 6. 踩坑

1. **bootstrap 混用**：宿主机造数必须 `localhost:9094`；作业默认 `kafka:9092`。写反则「topic 有数据、作业不消费」或反之。
2. **Pattern 无 `within`**：部分匹配永不释放，状态膨胀；Wave 0 单测已强制 `within(30s)`。
3. **watermark 停滞**：多分区时空闲分区会卡住水位；作业配置 `withIdleness(30s)`。造数须在 DTC 后追加足够晚的 HEARTBEAT，把水位推过 DTC（BoundedOutOfOrderness=5s），否则 MATCH 迟迟不落库。
4. **ClickHouse Sink flush**：SinkV2 先记 `batchSize` 再 `clear`；非 2xx 抛 `IOException`。表未建时作业会失败——先 `make up-p03`。
5. **MinIO / S3a 凭证链**：集群 checkpoint 落 `s3://flink/...`。须使用 `SimpleAWSCredentialsProvider`，否则 InstanceProfile 探测会让作业长时间停在 `INITIALIZING`（见 `docker/docker-compose.yml` 的 `FLINK_PROPERTIES`）。
6. **端口冲突**：本机其他 MinIO 占用 `9000` 时 `fll-minio` 起不来，JM 依赖 `minio-init` 会失败。先释放 `9000` 或停掉冲突容器。

## 7. 最佳实践

- 每条 CEP 模式登记五元组：业务含义 / within / 连接语义 / skip 策略 / 状态上界（本 Phase 仅一条 harsh→fault）。
- 验证脚本固定表名与查询，禁止拼接外部输入；Kafka 消费只做诊断日志。
- 有意义算子一律显式 `.uid(...)`，为后续 Savepoint 升级留契约。
- MATCH 与 TIMEOUT 同表同 topic，用 `alert_type` 区分，避免双表漂移。
- profile 隔离：业务 init 进 `p03`，永不污染 default `make up`。

## 8. 面试题与参考

**面试题**

1. CEP 的 `within` 超时由什么时钟驱动？watermark 停滞时会出现什么现象？
2. `followedBy` 与 interval join 都能表达「A 后 B」，何时必须选 CEP？
3. 为什么本项目的 `verify.sh` 不能用「Kafka 有消息」单独放行？
4. Side Output 的 TIMEOUT 与主流 MATCH 共用一张表时，如何避免语义混淆？

**参考**

- 教材 [docs/10-cep](../../docs/10-cep/README.md)；案例 [examples/e10-cep](../../examples/e10-cep/README.md)（C3/C5）
- Flink 2.2 CEP Pattern API / TimedOutPartialMatchHandler（官方文档）
- 本仓库 `docker/Makefile`（`up-p03` / `submit-p03`）、`sql/clickhouse_alerts.sql`
- ADR 完整内容延 Phase 3（VEH-07）；模块 15 登记见 [docs/README.md](../../docs/README.md)
