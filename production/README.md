# production/ · P5 生产化（OrbStack K8s）

在 OrbStack 内置 Kubernetes 上运行 **Flink Kubernetes Operator 1.15.0**，以 p03 `VehicleAlertJob` 的 `FlinkBlueGreenDeployment` 完成可观察 Blue/Green 演练，并用 **唯一 GitOps=Argo CD** + GitHub Actions 把门禁接上。版本坐标见根 README 版本矩阵。

## 已交付

| 路径 | 说明 |
|------|------|
| `scripts/check_env.sh` | Helm + OrbStack K8s Ready 门禁 |
| `scripts/install-operator.sh` | Helm 安装 Operator 1.15.0（`webhook.create=false`） |
| `scripts/probe_kafka_from_k8s.sh` | 锁定 bootstrap=`host.docker.internal:9095` |
| `scripts/run-bluegreen-drill.sh` | BG 演练；写出 `docs/bluegreen-timeline.md` |
| `scripts/install-argocd.sh` | Helm 安装 Argo CD 10.1.4 + 登记 Application |
| `scripts/verify-argocd-sync.sh` | Application Synced/Healthy 门禁（PROD-03） |
| `charts/p03-vehicle-alert/` | `FlinkBlueGreenDeployment` + Flink SA RBAC |
| `argocd/application-p03.yaml` | Argo Application（path → p03 chart） |
| `docker/p03-k8s-image/Dockerfile` | 本地 Application 镜像（`flinklab/p03-vehicle-alert:dev`） |
| `docs/operator-install.md` | Operator 安装 SOP |
| `docs/bluegreen-sop.md` | 部署 / 观察 / 切换 / 回滚 |
| `docs/bluegreen-timeline.md` | 演练脚本产物（命令驱动证据） |
| `docs/gitops-cicd.md` | Operator → BG → Argo sync → 回滚 → CI 总清单 |
| `../.github/workflows/ci.yml` | JDK 21 编译 + `scripts/qa_check.sh` |

## 快速复现

```bash
# 0) 环境
orb start k8s
bash production/scripts/check_env.sh
cd docker && docker compose up -d kafka clickhouse minio minio-init && cd ..

# 1) Operator
bash production/scripts/install-operator.sh

# 2) 镜像
make -C projects/p03-vehicle-monitoring package
docker build -f production/docker/p03-k8s-image/Dockerfile \
  -t flinklab/p03-vehicle-alert:dev \
  -t flinklab/p03-vehicle-alert:dev-green .

# 3) Blue/Green 演练（exit 0 + 时间线）——可选；也可直接走 Argo
bash production/scripts/run-bluegreen-drill.sh

# 4) Argo CD（唯一 GitOps）+ sync 门禁
bash production/scripts/install-argocd.sh
bash production/scripts/verify-argocd-sync.sh
# 期望：ok sync=Synced health=Healthy …

# 5) CI 门禁（本地等价；GHA 见 .github/workflows/ci.yml）
bash scripts/qa_check.sh
```

端到端勾选表与回滚、私有库凭据、可选 buildx：**[docs/gitops-cicd.md](./docs/gitops-cicd.md)**。

混合拓扑说明：Kafka 使用 compose **K8S** listener（宿主机 `9095`，advertised `host.docker.internal:9095`）；宿主机造数仍用 `localhost:9094`。详见 `docs/operator-install.md`。

## GitOps 声明

| 项 | 选择 |
|----|------|
| GitOps | **仅 Argo CD**（`install-argocd.sh` + `argocd/application-p03.yaml`） |
| CI | GitHub Actions（`.github/workflows/ci.yml`） |
| 验收脚本 | [`scripts/verify-argocd-sync.sh`](./scripts/verify-argocd-sync.sh) |

本目录**不**展开第二套 GitOps 工具对照，也不把多架构镜像推送写成硬门禁。

## 范围备注

- HA / uid / savepoint 纪律与 [`best-practice/`](../best-practice/) 互链：规范正文见 [`best-practice/02-uid-savepoint.md`](../best-practice/02-uid-savepoint.md)、[`07-cicd-gitops.md`](../best-practice/07-cicd-gitops.md)；**本目录只放可执行清单与 SOP**。
- Autoscaler、Session 模式仅见 `docs/bluegreen-sop.md` 附录，不作为 PROD-02 硬门禁。
- 三种 `upgradeMode`（stateless / savepoint / last-state）对照见 SOP 附录；硬门禁始终是 BG 状态机时间线。
