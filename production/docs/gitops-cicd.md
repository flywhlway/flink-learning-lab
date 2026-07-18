# GitOps + CI 可复现清单（Argo CD · GitHub Actions）

PROD-03 / D-07 / D-08 / D-09：在 OrbStack arm64 上走通**唯一** GitOps 路径（Argo CD）与 CI 门禁（GitHub Actions）。  
本清单可按顺序在本机跟完；**不**引入第二套 GitOps 工具，也**不**把多架构镜像推送当作硬门禁。

版本坐标（SSOT）：根 `README.md` 版本矩阵 — Argo CD Helm chart **10.1.4**（app `v3.4.5`），repo `https://argoproj.github.io/argo-helm`，chart `argo/argo-cd`。

## 端到端勾选表

| # | 步骤 | 命令 | 预期可观察输出 | 勾选 |
|---|------|------|----------------|------|
| 1 | OrbStack K8s Ready | `orb start k8s && kubectl config use-context orbstack && bash production/scripts/check_env.sh` | `ok helm=v4.2.3… k8s=Ready` | ☐ |
| 2 | compose 基座 | `cd docker && docker compose up -d kafka clickhouse minio minio-init && cd ..` | Kafka `9095`（K8S listener）、CH `8123`、MinIO `9000` 监听 | ☐ |
| 3 | 安装 Operator | `bash production/scripts/install-operator.sh` | CRD `flinkbluegreendeployments.flink.apache.org` 存在；`flink-operator` Pod Ready | ☐ |
| 4 | 构建 Application 镜像 | 见下方「镜像 / chart 发布」 | 本地 tag `flinklab/p03-vehicle-alert:dev`（及可选 `:dev-green`） | ☐ |
| 5 | Blue/Green 演练（可选先 helm） | `bash production/scripts/run-bluegreen-drill.sh` 或按 [bluegreen-sop.md](./bluegreen-sop.md) | exit 0；[bluegreen-timeline.md](./bluegreen-timeline.md) 有 `ACTIVE_*` 时间线 | ☐ |
| 6 | 安装 Argo CD + Application | `bash production/scripts/install-argocd.sh` | `ok argocd chart=10.1.4 … sync=Synced`；打印 admin 密码读取命令（**勿**写入仓库） | ☐ |
| 7 | Sync / Health 门禁 | `bash production/scripts/verify-argocd-sync.sh` | `ok sync=Synced health=Healthy … path=production/charts/p03-vehicle-alert` | ☐ |
| 8 | 回滚演练 | 见下方「回滚」 | BG 状态机回到对侧 `ACTIVE_*` 或 Argo 回退 revision 后仍 Synced | ☐ |
| 9 | CI 门禁（本地等价） | `bash scripts/qa_check.sh`；GHA 见 `.github/workflows/ci.yml` | `== QA PASS ==`；workflow 含 JDK21 `mvn compile` + `qa_check` | ☐ |

## 1. Operator 安装

完整 SOP：[operator-install.md](./operator-install.md)

```bash
bash production/scripts/check_env.sh
bash production/scripts/install-operator.sh
kubectl get crd flinkbluegreendeployments.flink.apache.org
kubectl get pods -n flink-operator
```

Kafka bootstrap 锁定为 `host.docker.internal:9095`（compose **K8S** listener）。探针：

```bash
bash production/scripts/probe_kafka_from_k8s.sh
```

## 2. FlinkBlueGreenDeployment（p03 chart）

Chart 路径：`production/charts/p03-vehicle-alert`（`FlinkBlueGreenDeployment` + flink SA RBAC）。

手工 Helm（学习 / 对照用）：

```bash
helm upgrade --install p03-vehicle-alert production/charts/p03-vehicle-alert \
  --namespace flink --create-namespace \
  --set image.tag=dev
```

一键 BG 演练与回滚步骤：[bluegreen-sop.md](./bluegreen-sop.md)。  
硬门禁是 CR 状态机时间线，不是单独改 `upgradeMode`。

## 3. Argo CD 安装与 Application

### 3.1 一键安装

```bash
bash production/scripts/install-argocd.sh
```

脚本行为：

1. Helm 安装 `argo/argo-cd` **10.1.4** 到 namespace `argocd`（`server.insecure=true`，ClusterIP）
2. 检测 `git remote get-url origin`：若 remote **已包含** `production/charts/p03-vehicle-alert`，则 Application `repoURL` 用 origin（HTTPS 规范化）
3. 否则（常见：chart 仅在本地分支、尚未 push）：在 `.gitops-mirror/` 建 bare 仓库，并在集群部署 **git-daemon**（Service `gitops-mirror:9418`，hostPath 挂载本机镜像目录），Application 使用  
   `git://gitops-mirror.argocd.svc.cluster.local/flink-learning-lab.git`，`targetRevision=dev`
4. `kubectl apply` [`production/argocd/application-p03.yaml`](../argocd/application-p03.yaml)（改写后的 repoURL），并触发**显式 sync**（默认无 automated sync）
5. 打印读取 initial admin 密码的命令（从 Secret `argocd-initial-admin-secret`；**禁止**把密码写入仓库或文档正文）

Application 要点：

| 字段 | 值 |
|------|-----|
| `metadata.name` | `flinklab-p03-bg` |
| `source.path` | `production/charts/p03-vehicle-alert` |
| `destination.namespace` | `flink` |
| `syncPolicy` | 手动/显式 sync（威胁模型：防误 sync） |

### 3.2 私有 GitHub 凭据（origin 路径）

当选择 origin 且仓库为 private 时：

1. 在 GitHub 创建 **fine-grained PAT**（Contents: Read）或 deploy key（只读）
2. 登记到 Argo（任选其一）：
   - UI：Settings → Repositories → Connect Repo（HTTPS + 用户名/PAT，或 SSH）
   - CLI 等价（若已装 `argocd` CLI）：`argocd repo add <HTTPS_URL> --username <u> --password <PAT>`
3. Secret 形态（示意，**勿**提交真实 token）：

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: repo-flink-learning-lab
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository
stringData:
  type: git
  url: https://github.com/<org>/flink-learning-lab.git
  username: <github-user>
  password: <PAT>
```

本学习工程默认用本机 git-daemon 镜像，**不**要求配置 GitHub PAT 即可让 `verify-argocd-sync.sh` 绿。

### 3.3 验收

```bash
bash production/scripts/verify-argocd-sync.sh
# 期望：ok sync=Synced health=Healthy app=flinklab-p03-bg path=production/charts/p03-vehicle-alert …
kubectl get application -n argocd
kubectl get flinkbluegreendeployment -n flink
```

等价判据：`health=Progressing` 在 Flink CR 调谐窗口可接受（脚本默认 `Healthy|Progressing`）；稳定后应为 `Healthy`。

UI（可选）：

```bash
kubectl -n argocd port-forward svc/argocd-server 8080:443
# 浏览器 https://localhost:8080 ；用户 admin；密码见 install 脚本打印的 kubectl 命令
```

## 4. 回滚

### 4.1 作业层（Blue/Green 状态机）

按 [bluegreen-sop.md](./bluegreen-sop.md)「回滚」：再次变更 `image.tag`（或 values）触发对侧 `ACTIVE_*`。  
GitOps 路径下应**先改 Git（或本机 mirror）再 sync**，而不是只 `kubectl edit`。

本机 mirror 刷新后再 sync：

```bash
# 修改 production/charts/p03-vehicle-alert/values.yaml（例如 image.tag）后提交
git add production/charts/p03-vehicle-alert && git commit -m "chore: bump p03 image tag for rollback drill"
bash production/scripts/install-argocd.sh   # 会刷新 .gitops-mirror 并显式 sync
# 或仅：
git push --force .gitops-mirror/flink-learning-lab.git HEAD:refs/heads/dev
git --git-dir=.gitops-mirror/flink-learning-lab.git update-server-info
kubectl -n argocd patch application flinklab-p03-bg --type merge \
  -p '{"operation":{"initiatedBy":{"username":"rollback"},"sync":{"revision":"dev"}}}'
bash production/scripts/verify-argocd-sync.sh
```

### 4.2 GitOps 层（回退 Application revision）

```bash
# 查看历史（UI History，或）
kubectl -n argocd get application flinklab-p03-bg -o jsonpath='{.status.history}' ; echo
# 将 targetRevision 指回已知好 commit / tag 后显式 sync
kubectl -n argocd patch application flinklab-p03-bg --type merge \
  -p '{"spec":{"source":{"targetRevision":"<good-sha-or-tag>"}},"operation":{"sync":{"revision":"<good-sha-or-tag>"}}}'
bash production/scripts/verify-argocd-sync.sh
```

## 5. GitHub Actions CI

工作流：[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)

| 步骤 | 说明 |
|------|------|
| JDK 21 + Maven | `examples/` 下 `mvn -B -T1C -DskipTests compile` |
| `bash scripts/qa_check.sh` | compose 可解析、违禁词、断链、案例计数（与仓库门禁同语义） |

触发：`push` / `pull_request` 到 `main` / `master` / `dev`。

本地等价（无 GitHub 也可验收门禁语义）：

```bash
cd examples && mvn -B -T1C -DskipTests compile && cd ..
bash scripts/qa_check.sh
```

CI **不**要求：登录 OrbStack、推送集群、持有 registry 账号。

## 6. 镜像 / chart 发布（文档化；非硬门禁）

本机 tag（arm64 / OrbStack）：

```bash
make -C projects/p03-vehicle-monitoring package
docker build -f production/docker/p03-k8s-image/Dockerfile \
  -t flinklab/p03-vehicle-alert:dev \
  -t flinklab/p03-vehicle-alert:dev-green .
# chart：直接使用仓库内 production/charts/p03-vehicle-alert（Helm / Argo path）
```

### 可选：多架构 buildx 推送（非 PROD-03 硬门禁）

仅在已具备远端 registry 账号时使用；**失败或未配置不得阻塞**本清单第 7 步验收。

```bash
# 示例（需自行替换 REGISTRY；非 CI required job）
docker buildx build --platform linux/amd64,linux/arm64 \
  -f production/docker/p03-k8s-image/Dockerfile \
  -t ${REGISTRY}/flinklab/p03-vehicle-alert:dev \
  --push .
```

## 7. 单一路径声明

| 项 | 本仓库选择 |
|----|------------|
| GitOps | **仅 Argo CD** |
| CI | **GitHub Actions**（`.github/workflows/ci.yml`） |
| 演示环境 | OrbStack arm64 Kubernetes |
| 明确不做 | 第二套 GitOps 对照教材；强制多架构推送；把密码写入仓库 |

## 相关入口

- [production/README.md](../README.md) — 总入口
- [operator-install.md](./operator-install.md) — Operator
- [bluegreen-sop.md](./bluegreen-sop.md) — BG 操作与回滚
- [../scripts/install-argocd.sh](../scripts/install-argocd.sh) / [../scripts/verify-argocd-sync.sh](../scripts/verify-argocd-sync.sh)
- [../argocd/application-p03.yaml](../argocd/application-p03.yaml)
