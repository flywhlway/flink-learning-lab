# 05 · 反压基线

## 规则

1. **上线前压测** 得到「开始持续反压」的 eps 与瓶颈算子（总则 #10）。数字写入 [`benchmark/baseline.md`](../benchmark/baseline.md) 或项目 `docs/baseline.md`。
2. **值班先看五指标：** `backPressuredTimeMsPerSecond` / `busyTimeMsPerSecond` / `lastCheckpointDuration` / `numRestarts` / `currentEmitEventTimeLag`（见 [`monitoring/README.md`](../monitoring/README.md)）。
3. **治理顺序：** 定位最忙下游 → 外部依赖 → 状态/序列化 → 倾斜 → 最后才扩容。
4. **禁止** 用加大 checkpoint timeout 掩盖对齐过久。

## 理由

没有基线的「反压告警」无法判断是流量打高还是代码回退；扩容是最后手段。

## 反例

- 未跑矩阵就宣称「支持 20k eps」。
- 只加 parallelism，忽略单 key 热点，反压依旧。

## 落地互链

- 仓库级矩阵：[`benchmark/`](../benchmark/)
- Grafana：[`monitoring/platform-overview.json`](../monitoring/platform-overview.json)、[`job-deepdive.json`](../monitoring/job-deepdive.json)
- 模块 13：[`docs/13-performance/README.md`](../docs/13-performance/README.md)
