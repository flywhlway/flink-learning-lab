# production/ · P5 生产化（OrbStack K8s）

在 OrbStack 内置 Kubernetes 上运行 **Flink Kubernetes Operator 1.15.0**，以 p03 `VehicleAlertJob` 的 `FlinkBlueGreenDeployment` 完成可观察 Blue/Green 演练。版本坐标见根 README 版本矩阵。

## 当前已交付（PROD-02）

| 路径 | 说明 |
|------|------|
| `scripts/check_env.sh` | Helm + OrbStack K8s Ready 门禁 |
| `scripts/install-operator.sh` | Helm 安装 Operator 1.15.0（`webhook.create=false`） |
| `scripts/probe_kafka_from_k8s.sh` | 锁定 bootstrap=`host.docker.internal:9095` |
| `scripts/run-bluegreen-drill.sh` | BG 演练；写出 `docs/bluegreen-timeline.md` |
| `charts/p03-vehicle-alert/` | `FlinkBlueGreenDeployment` + Flink SA RBAC |
| `docker/p03-k8s-image/Dockerfile` | 本地 Application 镜像（`flinklab/p03-vehicle-alert:dev`） |
| `docs/operator-install.md` | 安装 SOP |
| `docs/bluegreen-sop.md` | 部署 / 观察 / 切换 / 回滚 |
| `docs/bluegreen-timeline.md` | 演练脚本产物（命令驱动证据） |

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

# 3) Blue/Green 演练（exit 0 + 时间线）
bash production/scripts/run-bluegreen-drill.sh
```

混合拓扑说明：Kafka 使用 compose **K8S** listener（宿主机 `9095`，advertised `host.docker.internal:9095`）；宿主机造数仍用 `localhost:9094`。详见 `docs/operator-install.md`。

## 下一波（PROD-03，本目录预留）

GitOps（Argo CD）与 CI 路径在后续 plan 落地；入口脚本骨架见 `scripts/verify-argocd-sync.sh`（当前为未装 Argo 时的失败态 harness）。本 README **不**展开 Flux，也不把 GitOps 细节写成占位句。

## 范围备注

- HA / uid / savepoint 纪律与 `best-practice/` 互链；规范正文在 `best-practice/`，此处放可执行清单。
- Autoscaler、Session 模式仅见 `docs/bluegreen-sop.md` 附录，不作为 PROD-02 硬门禁。
- 三种 `upgradeMode`（stateless / savepoint / last-state）对照见 SOP 附录；硬门禁始终是 BG 状态机时间线。
