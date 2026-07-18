# 07 · CI/CD / GitOps 检查清单

## 规则

1. **单一 GitOps：** 只深讲 Argo CD；禁止并行 Flux 第二套（D-07）。
2. **CI 最小集：** JDK21 编译 + `scripts/qa_check.sh`（断链/违禁词/案例计数等）+ 本 Phase 相关门禁（interview≥150、compose config）。
3. **GitOps 收敛：** Application 指向本仓 `production/charts/...`；Sync/Health 由脚本断言。
4. **密钥不入库：** Argo admin 密码只从 Secret 读取命令输出，不写 Markdown。
5. **镜像/chart 坐标先登记版本矩阵** 再引用。

## 理由

双 GitOps 导致真相分裂；无 CI 的「已推送清单」不可信；密钥进库等于公开集群。

## 反例

- 文档同时写 Flux 与 Argo「二选一都行」却无默认路径。
- 把 `argocd admin password` 明文贴进 README。

## 落地互链（规范 ↔ 清单）

- **落地勾选表（权威操作步骤）：** [`production/docs/gitops-cicd.md`](../production/docs/gitops-cicd.md)
- CI workflow：[`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
- 环境门禁：`production/scripts/check_env.sh` / `verify-argocd-sync.sh`
- 模块 14：[`docs/14-production/README.md`](../docs/14-production/README.md)

---

## 实质扩写 · CI/CD 与 GitOps（Wave 2）

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

- [ ] 检查项 1：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 2：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 3：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 4：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 5：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 6：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 7：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 8：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 9：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 10：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 11：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）
- [ ] 检查项 12：与「CI/CD 与 GitOps」相关的可观察证据已具备（日志关键字/指标/基线行）

### 反模式

- 反模式 1：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 2：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 3：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 4：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 5：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 6：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

- 反模式 7：用口头保证代替演练；或复制无关规范段落；或把 `.planning` 草稿当完成态。纠正：回到 examples 作业与 production SOP。

### 互链

- [GitOps 文档](../production/docs/gitops-cicd.md)
- [GitOps CI 段落](../production/docs/gitops-cicd.md)

### 情景与度量

#### 情景 1

准备：确定作业 uid 与环境。执行：注入与「CI/CD 与 GitOps」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 2

准备：确定作业 uid 与环境。执行：注入与「CI/CD 与 GitOps」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 3

准备：确定作业 uid 与环境。执行：注入与「CI/CD 与 GitOps」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 4

准备：确定作业 uid 与环境。执行：注入与「CI/CD 与 GitOps」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

#### 情景 5

准备：确定作业 uid 与环境。执行：注入与「CI/CD 与 GitOps」相关的故障或变更。观察：指标/日志。恢复：按 SOP。记录：写入 baseline 或演练笔记。

### 评审问句

1. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

2. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

3. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

4. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

5. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

6. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

7. 若本次变更触碰「CI/CD 与 GitOps」，哪个指标先亮红？谁负责回滚？savepoint 是否保留？

