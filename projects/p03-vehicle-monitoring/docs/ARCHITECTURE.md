# p03 · 架构短文（告警 + 窗口大盘 + 演练）

> 对应教材：[docs/10-cep](../../../docs/10-cep/README.md) · [docs/02-time-window](../../../docs/02-time-window/README.md) · 值班指标 [monitoring/README.md](../../../monitoring/README.md)
> 决策记录：[ADR-0001](adr/0001-cep-broadcast-precompiled.md) · 模式五元组：[PATTERN-LIBRARY.md](PATTERN-LIBRARY.md)

## 1. 边界

本页描述 **车联网监控样板** 在 OrbStack arm64 上可复现的运行拓扑：CEP 告警作业、旁路窗口指标作业、Grafana 双数据源大盘、以及两条演练入口。不覆盖 p01/p02、P5 Operator Blue/Green、仓库级 `benchmark/` 全矩阵。

## 2. 总图

```text
                    ┌─────────────────────────────┐
                    │ gen_vehicle_events.py       │
                    │  (--scenario | --rate/--dur │
                    │   | stall/recover modes)    │
                    └──────────────┬──────────────┘
                                   │ produce / control
                                   ▼
                          Kafka vehicle.events
                          Kafka vehicle.pattern.control
                     ┌─────────────┴──────────────┐
                     │                            │
                     ▼                            ▼
        VehicleAlertJob（CEP 主图）     VehicleWindowMetricsJob（旁路）
        groupId=p03-vehicle-alert       groupId=p03-window-metrics
        WM: ooo=5s, idleness=30s        同 WM；uid 前缀 p03-wm-
        3× CEP → PatternActivationGate  keyBy(vin) → 30s tumbling
        → CH + Kafka alerts             → CH HTTP SinkV2
                     │                            │
                     ▼                            ▼
        CH flinklab.vehicle_alerts      CH flinklab.vehicle_window_metrics
        （CEP 权威 / pattern_id）       （大盘业务窗口指标）
                     │                            │
                     └────────────┬───────────────┘
                                  ▼
                     Grafana p03-vehicle-overview
              ┌───────────────────┴────────────────────┐
              │ Prometheus DS                          │ ClickHouse DS
              │ Flink 健康（反压/ckpt/重启/lag）       │ 窗口吞吐、MATCH 速率、
              │ 见 monitoring/README 值班五指标         │ 异常阈值面板
              └───────────────────┬────────────────────┘
                                  │
              verify.sh（CH CEP 权威）
              verify_dashboard.sh（JSON + Grafana API + CH metrics）
              loadtest.sh → docs/baseline.md
              drill_watermark_stall.sh（冻结 HEARTBEAT → recover verify）
```

## 3. 两条 Flink 作业（解耦纪律）

| 作业 | 主类 | Kafka `group.id` | 职责 |
|------|------|------------------|------|
| 告警 | `VehicleAlertJob` | `p03-vehicle-alert`（默认） | 静态三路 CEP + Broadcast 门控；双写 `vehicle_alerts` / `vehicle.alerts` |
| 窗口 | `VehicleWindowMetricsJob` | `p03-window-metrics` | 旁路 tumbling 聚合；写入 `vehicle_window_metrics` |

窗口聚合 **不得** 塞进 CEP 主图（D-02）：门控语义、CEP 状态上界与大盘吞吐解耦。提交入口：`make submit` / `make submit-window`（或 `docker` 侧 `make submit-p03` / `make submit-p03-window`）。

CEP 动态化路线锁定为 **编译期 Pattern + Broadcast 选择预编译激活集**，见 [ADR-0001](adr/0001-cep-broadcast-precompiled.md)；教材对照 [docs/10-cep §10-05](../../../docs/10-cep/README.md)。

## 4. 可观测与大盘

- **业务面板**：ClickHouse 表查询（Grafana 插件 `grafana-clickhouse-datasource`，uid `p03-clickhouse`）。
- **平台健康**：Prometheus → Flink reporter；查询思路对齐 [monitoring/README.md](../../../monitoring/README.md) 值班五指标。
- **可导入 JSON**：`monitoring/dashboards/p03-vehicle-overview.json`（provisioning，不绑 `--profile p03`）。
- **异常检测**：Grafana 阈值面板 + [ANOMALY-THRESHOLDS.md](ANOMALY-THRESHOLDS.md)；不扩 CEP、不新增 ML 作业。
- **门禁**：`make verify-dashboard` → `scripts/verify_dashboard.sh`。

浏览器：`http://localhost:3000/d/p03-vehicle-overview/p03-vehicle-overview`（`admin` / `flinklab`）。

## 5. 演练与验收权威

| 入口 | 脚本 | 断言重心 |
|------|------|----------|
| CEP 回归 | `scripts/verify.sh` / `make verify` | ClickHouse `vehicle_alerts` 的 `pattern_id` MATCH；**Kafka 仅诊断** |
| Broadcast 切换 | `make verify-switch` | TRUNCATE → control → gen → `PATTERN_ID` verify |
| 大盘 | `make verify-dashboard` | JSON 非空 + Grafana API + CH metrics smoke |
| 压测 baseline | `make loadtest` → [baseline.md](baseline.md) | Prometheus + CH 摘要（OrbStack 实测） |
| watermark 停滞 | `make drill-watermark` | 冻结 HEARTBEAT 停滞 → recover 后 CH MATCH；时间语义见 [docs/02-time-window](../../../docs/02-time-window/README.md) |

Compose 隔离：`profiles: ["p03"]` 的 `p03-init` 创建 topic 与 DDL；default `make up` 不要求 `--profile p03`。

## 6. 交叉引用

- 八段式启动与验收步骤：项目根 [README.md](../README.md)
- 模式库五元组与门控踩坑：[PATTERN-LIBRARY.md](PATTERN-LIBRARY.md)
- 仓库压测方法论子集：[benchmark/README.md](../../../benchmark/README.md)（全矩阵属 P5）
