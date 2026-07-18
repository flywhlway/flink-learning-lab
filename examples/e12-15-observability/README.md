# e12-15 · Streaming Observability:Metrics + 结构化日志

> 对应 [ai/chapters/15-streaming-observability.md](../../ai/chapters/15-streaming-observability.md) · Level:L4
> 运行:`mvn -q -Plocal compile exec:java -pl e12-15-observability -Dexec.mainClass=com.flywhl.flinklab.e12.ObservableAgentJob`

## 背景

不依赖 Flink Agents 框架自带的 EventLog,任何 DataStream 算子都能用本 Demo 的模式接入完整可观测性:自定义 Counter/Histogram + 结构化日志。本仓库从 Phase 0 就搭好的 Prometheus/Grafana 管道在此直接复用。

## 验证方式

本地运行观察控制台的结构化日志行(`agent_decision event_user=... latency_ms=...`);提交到集群后可在 Grafana/WebUI 看到 `alerts_triggered` 与 `decision_latency_ms` 指标。

## 源码要点

- `DropwizardHistogramWrapper` 是 Flink 官方提供的桥接类,把 Dropwizard Metrics 的 Histogram 接入 Flink Metrics 体系。
- 结构化日志字段命名建议全仓库统一(event_user/action/latency_ms 等),便于跨作业聚合分析。

## 面试题

见 ai/chapters/15-streaming-observability.md 第 8 节。
