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

---

## 实质扩写 · uid / savepoint / 作业演进（Wave 2）

### 为何需要这条规范

1. 在真实作业生命周期（开发→联调→压测→上线→升级→故障）第 1 段，缺少本规范会导致可复核性下降：要么状态对不上，要么语义从 EO 退化为重复，要么排障无指标。仓库用 examples 证明机制，用 production/best-practice 固定纪律。

2. 在真实作业生命周期（开发→联调→压测→上线→升级→故障）第 2 段，缺少本规范会导致可复核性下降：要么状态对不上，要么语义从 EO 退化为重复，要么排障无指标。仓库用 examples 证明机制，用 production/best-practice 固定纪律。

3. 在真实作业生命周期（开发→联调→压测→上线→升级→故障）第 3 段，缺少本规范会导致可复核性下降：要么状态对不上，要么语义从 EO 退化为重复，要么排障无指标。仓库用 examples 证明机制，用 production/best-practice 固定纪律。

4. 在真实作业生命周期（开发→联调→压测→上线→升级→故障）第 4 段，缺少本规范会导致可复核性下降：要么状态对不上，要么语义从 EO 退化为重复，要么排障无指标。仓库用 examples 证明机制，用 production/best-practice 固定纪律。

5. 在真实作业生命周期（开发→联调→压测→上线→升级→故障）第 5 段，缺少本规范会导致可复核性下降：要么状态对不上，要么语义从 EO 退化为重复，要么排障无指标。仓库用 examples 证明机制，用 production/best-practice 固定纪律。

### 怎么做（可执行步骤）

1. 步骤 1：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

2. 步骤 2：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

3. 步骤 3：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

4. 步骤 4：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

5. 步骤 5：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

6. 步骤 6：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

7. 步骤 7：对照互链文档完成配置/代码/评审门禁；在 OrbStack 执行一次最小验证；把命令与期望写入 MR 描述。禁止只改文档不改作业。

### 检查清单

- [ ] 检查项 1：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 2：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 3：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 4：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 5：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 6：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 7：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 8：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 9：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 10：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 11：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 12：与「uid / savepoint / 作业演进」相关的可观察证据已具备（日志关键字/指标/基线行）

### 反模式

- 反模式 1：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 2：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 3：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 4：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 5：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 6：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 7：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

### 互链

- [Blue/Green SOP](../production/docs/bluegreen-sop.md)
- [e04](../examples/e04-checkpoint/README.md)

### 情景与度量

#### 情景 1

准备：确定作业 uid 与环境。执行：注入与「uid / savepoint / 作业演进」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 2

准备：确定作业 uid 与环境。执行：注入与「uid / savepoint / 作业演进」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 3

准备：确定作业 uid 与环境。执行：注入与「uid / savepoint / 作业演进」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 4

准备：确定作业 uid 与环境。执行：注入与「uid / savepoint / 作业演进」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 5

准备：确定作业 uid 与环境。执行：注入与「uid / savepoint / 作业演进」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

### 评审问句

1. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

2. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

3. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

4. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

5. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

6. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

7. 若本次变更触碰「uid / savepoint / 作业演进」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

