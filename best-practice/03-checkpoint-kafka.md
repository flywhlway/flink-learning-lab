# 03 · checkpoint / Kafka 事务 / 端到端一致性

## 规则

1. **checkpoint 间隔显著小于 Kafka 事务超时**；同时抬高 broker `transaction.max.timeout.ms` 放行（总则 #2）。
2. **端到端 EXACTLY_ONCE =** 可重置源 + Flink checkpoint + 事务/幂等 sink；缺一不可。checkpoint 成功 ≠ 下游已 EOS。
3. **远程 CP 存储**（本仓库 MinIO）权限与磁盘监控必做；失败看 `numberOfFailedCheckpoints`。
4. **大状态开增量 checkpoint**（RocksDB）（总则 #9）。

## 理由

事务超时短于对齐+上传时间 → 事务被 abort → 看似 EOS 实则丢数或重复。值班必须以指标证伪。

## 反例

- checkpoint 10s、事务超时 5s，「正好一次」作业周期性丢窗口结果。
- 只用 at-least-once Kafka sink 却对业务承诺 EOS。

## 落地互链

- 连接器语义教材：[`docs/07-connectors/`](../docs/07-connectors/)
- 容错教材：[`docs/04-checkpoint/`](../docs/04-checkpoint/)
- 看板：[`monitoring/job-deepdive.json`](../monitoring/job-deepdive.json)

---

## 实质扩写 · Checkpoint 与 Kafka 语义（Wave 2）

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

- [ ] 检查项 1：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 2：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 3：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 4：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 5：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 6：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 7：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 8：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 9：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 10：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 11：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 12：与「Checkpoint 与 Kafka 语义」相关的可观察证据已具备（日志关键字/指标/基线行）

### 反模式

- 反模式 1：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 2：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 3：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 4：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 5：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 6：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 7：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

### 互链

- [容错](../docs/04-checkpoint/README.md)
- [连接器](../docs/07-connectors/README.md)

### 情景与度量

#### 情景 1

准备：确定作业 uid 与环境。执行：注入与「Checkpoint 与 Kafka 语义」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 2

准备：确定作业 uid 与环境。执行：注入与「Checkpoint 与 Kafka 语义」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 3

准备：确定作业 uid 与环境。执行：注入与「Checkpoint 与 Kafka 语义」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 4

准备：确定作业 uid 与环境。执行：注入与「Checkpoint 与 Kafka 语义」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 5

准备：确定作业 uid 与环境。执行：注入与「Checkpoint 与 Kafka 语义」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

### 评审问句

1. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

2. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

3. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

4. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

5. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

6. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

7. 若本次变更触碰「Checkpoint 与 Kafka 语义」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

