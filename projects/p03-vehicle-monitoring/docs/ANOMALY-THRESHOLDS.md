# p03 · 异常检测阈值条文（Grafana，非 ML / 非新 CEP）

> **范围（D-04 / D-05）：** 异常检测落地为 Grafana 阈值面板 + 本文档；不新增 ML 作业，不扩展 CEP 模式库。  
> **数字性质：** 下列数值均为 **演示默认 / 非生产 SLA**，供 OrbStack 学习复现；未经 `loadtest.sh` 校准前不得当作生产告警线。

大盘 JSON：[monitoring/dashboards/p03-vehicle-overview.json](../monitoring/dashboards/p03-vehicle-overview.json)  
验证：`make verify-dashboard`（`scripts/verify_dashboard.sh`）

---

## 规则 1：近 5 分钟 MATCH 激增

| 项 | 内容 |
|---|---|
| 含义 | 单位时间内 MATCH 告警条数异常偏高，提示造数/压测激增或误报风暴 |
| 查询来源 | ClickHouse `flinklab.vehicle_alerts` |
| SQL（与面板一致） | `SELECT countIf(alert_type = 'MATCH') FROM flinklab.vehicle_alerts WHERE event_time >= now64(3) - INTERVAL 5 MINUTE` |
| 演示默认阈值 | **≥ 5** → 面板黄/红（stat 面板 thresholds：3=yellow，5=red） |
| Grafana 面板 | 「异常阈值 · 近 5 分钟 MATCH 数（演示阈值 ≥5）」 |
| 标注 | **演示默认 / 非生产 SLA** |

---

## 规则 2：vin 窗口 harsh_count 越界

| 项 | 内容 |
|---|---|
| 含义 | 单 vin 在一个滚动窗口内急加速计数过高 |
| 查询来源 | ClickHouse `flinklab.vehicle_window_metrics` |
| SQL（与面板一致） | `SELECT max(harsh_count) FROM flinklab.vehicle_window_metrics WHERE window_start >= now64(3) - INTERVAL 10 MINUTE` |
| 演示默认阈值 | **≥ 3** → 面板橙/红（thresholds：3=orange，5=red） |
| Grafana 面板 | 「异常阈值 · vin 窗口 max(harsh_count)（演示阈值 ≥3）」 |
| 标注 | **演示默认 / 非生产 SLA**；窗口作业默认 **30s** tumbling（见 README） |

---

## 安全与运维说明

- Grafana ClickHouse 数据源使用 lab 账号 `flinklab`（见 compose provisioning）；**生产应改为只读账号**（T-03-05 accept）。
- dashboard JSON **不含密码**；凭据仅在 `docker/config/grafana/provisioning/datasources/clickhouse.yml`。
- 阈值若经压测改写，须同步改本文件与 JSON 面板 `thresholds.steps`，并在 `docs/baseline.md`（Phase 03-02）记录实测依据。
