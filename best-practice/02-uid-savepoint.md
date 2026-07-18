# 02 · uid / savepoint / 作业演进

## 规则

1. **每个有状态算子显式 `.uid("stable-id")` 与 `.name(...)`**（总则 #1）。uid 一经生产使用视为公共 API。
2. **拓扑演进白名单：** 可改无状态 map；新增有状态算子必须新 uid；删除有状态算子需明确弃状态或做 State Processor 迁移。
3. **升级路径：** `stop-with-savepoint`（或 Operator 等价）→ 保留至少 3 份 → 从 savepoint 启新版本；禁止生产 `cancel` 当升级（总则 #8）。
4. **maxParallelism** 在首次上线论证，避免日后无法 rescale。

## 理由

Savepoint/Checkpoint 按 uid 匹配状态；自动生成 uid 随插入算子而变，表现为「恢复后指标归零」或启动失败。

## 反例

- 为「代码整洁」重命名包并重提作业，未保 uid → 全量状态作废。
- Blue/Green 切到新镜像但忘记 `--fromSavepoint`，业务窗口从空状态开始。

## 落地互链

- Blue/Green SOP：[`production/docs/bluegreen-sop.md`](../production/docs/bluegreen-sop.md)
- 演练时间线证据：[`production/docs/bluegreen-timeline.md`](../production/docs/bluegreen-timeline.md)
