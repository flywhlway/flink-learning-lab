# 模块 13 · 性能与压测

> 背景:仓库级压测矩阵与基线报告是 P5 生产化的硬门禁——数字必须来自 OrbStack arm64 实测，禁止虚构吞吐。
> 权威路径:[benchmark/](../../benchmark/)（方法论、`scripts/run_matrix.sh`、权威报告 [`baseline.md`](../../benchmark/baseline.md)）
> 状态:✅ 完成（PROD-01：compose Flink 裁剪矩阵 + OrbStack 实测 `benchmark/baseline.md`）

## 本章入口

- 压测方法论与裁剪矩阵轴（D-01）→ [benchmark/README.md](../../benchmark/README.md)
- 矩阵入口 → `make -C benchmark matrix` / `make -C benchmark dry-run`
- 仓库级基线报告 → [`benchmark/baseline.md`](../../benchmark/baseline.md)

## 与项目级 baseline 的关系

`projects/*/docs/baseline.md` 可交叉引用，**不**替代仓库级 [`benchmark/baseline.md`](../../benchmark/baseline.md)（D-03）。

## 必跑口径摘要

| 轴 | 取值 |
|---|---|
| 作业 | e01-J2 / e10 `C5VehicleDtcPatternJob` / p03 `VehicleAlertJob` |
| 负载 | 1k / 5k eps 必跑；20k stretch（可 SKIPPED） |
| State | HashMap + RocksDB 增量 |
| Checkpoint | 对齐 30s 主路径 + 对照行 |
| 部署 | compose Flink（非 K8s） |
