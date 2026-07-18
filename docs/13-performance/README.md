# 模块 13 · 性能与压测

> 背景:仓库级压测矩阵与基线报告是 P5 生产化的硬门禁——数字必须来自 OrbStack arm64 实测，禁止虚构吞吐。
> 权威路径:[benchmark/](../../benchmark/)（方法论、`scripts/run_matrix.sh`、目标产物 `baseline.md`）
> 状态:🚧 P5 进行中（Wave 0 已登记编号与失败态 harness；完整矩阵在后续 wave 实测）

## 本章入口

- 压测方法论与报告模板 → [benchmark/README.md](../../benchmark/README.md)
- 矩阵入口脚本 → `benchmark/scripts/run_matrix.sh`（未实现完整矩阵时非 0）
- 仓库级基线报告 → `benchmark/baseline.md`（矩阵跑通后生成）

## 与项目级 baseline 的关系

`projects/*/docs/baseline.md` 可交叉引用，不替代仓库级 `benchmark/baseline.md`。
