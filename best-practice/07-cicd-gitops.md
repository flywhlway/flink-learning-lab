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
