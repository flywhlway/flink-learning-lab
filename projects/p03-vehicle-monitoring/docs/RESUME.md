# p03 · 简历陈述页（可复现）

> 每条陈述对应仓库内命令或路径。数字只引用 [baseline.md](baseline.md) / `verify` 实测；禁止空泛形容词。Lab 默认账号见项目 README（`admin` / `flinklab`）。

## 一句话

在 OrbStack arm64 上交付车联网 CEP 告警样板：独立 compose profile `p03`、静态三模式 + Broadcast 切换、旁路窗口指标写入 ClickHouse、Grafana 双数据源大盘，并用脚本断言 ClickHouse 权威出口与 watermark 停滞演练。

## 可复现陈述（动词 → 路径）

| 陈述 | 复现命令 / 路径 |
|------|-----------------|
| 一键起 p03 topic 与 CH DDL，不影响 default `make up` | `cd docker && make up && make init && make up-p03` |
| 提交 CEP 告警作业与旁路窗口作业（独立 `group.id`） | `cd projects/p03-vehicle-monitoring && make package && make submit && make submit-window` |
| 默认激活 `HARSH_THEN_FAULT` 后，用 CH `pattern_id` 断言 MATCH | `make gen` → `PATTERN_ID=HARSH_THEN_FAULT make verify`（`scripts/verify.sh`） |
| 不重启作业，用控制消息切换激活集并按 `pattern_id` 验收 | `make verify-switch` |
| 打开 Grafana 双 DS 大盘并跑大盘门禁 | 浏览器 `http://localhost:3000/d/p03-vehicle-overview/p03-vehicle-overview`；`make verify-dashboard` |
| 跑项目级压测并留下 baseline 表 | `make loadtest` → [docs/baseline.md](baseline.md) |
| 演示 watermark 停滞后恢复 MATCH | `make drill-watermark`（`scripts/drill_watermark_stall.sh`） |
| 说明为何不用商业动态 CEP | [docs/adr/0001-cep-broadcast-precompiled.md](adr/0001-cep-broadcast-precompiled.md) |
| 说明告警与大盘解耦拓扑 | [docs/ARCHITECTURE.md](ARCHITECTURE.md) |

## 验收纪律（面试可答）

1. **CEP 权威出口是 ClickHouse**，不是 Kafka：`verify.sh` 只认 `flinklab.vehicle_alerts` 的 `alert_type='MATCH'` + `pattern_id`；Kafka 仅诊断（D-15）。
2. **Broadcast 关闸 ≠ CEP 状态停写**：三路 NFA 仍跑，靠 `within` TTL 回收；见 [PATTERN-LIBRARY.md](PATTERN-LIBRARY.md)。
3. **异常检测是 Grafana 阈值面板**，不是第二套 CEP；条文见 [ANOMALY-THRESHOLDS.md](ANOMALY-THRESHOLDS.md)。

## 实测数字摘录（仅 baseline）

来源：[docs/baseline.md](baseline.md)（`make loadtest` 于 2026-07-18 OrbStack arm64 写入；**非生产 SLA**）。

| 项 | 值 |
|----|-----|
| 负载 | 100 eps × 120s（热身 45s 丢弃） |
| 墙钟折算 produce rate | 100.0 eps |
| `lastCheckpointDuration`（p03 jobs，max） | 81.0 ms |
| `numRestarts`（p03 jobs，max） | 0.0 |
| `currentEmitEventTimeLag`（vehicle_events source，max） | 11.0 ms |

复跑：`cd projects/p03-vehicle-monitoring && make loadtest`。

## 技术栈锚点（SSOT）

Flink 2.2.1 · JDK 21 · Kafka connector 5.0.0-2.2 · ClickHouse 24.8 · Prometheus / Grafana（版本见仓库根 README 矩阵）。架构与交叉引用见 [ARCHITECTURE.md](ARCHITECTURE.md)。
