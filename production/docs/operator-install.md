# Flink Kubernetes Operator 1.15.0 安装（OrbStack arm64）

PROD-02 / D-04：在本机 OrbStack Kubernetes 上用 Helm 安装官方 Operator，并锁定 K8s→compose Kafka 的 bootstrap 写法。

## 前置

1. OrbStack 已安装；启用 Kubernetes：

```bash
orb start k8s
kubectl config use-context orbstack
kubectl get nodes   # 期望 Ready
```

2. Helm CLI（版本矩阵：**4.2.3**，Homebrew `helm`；禁止 pip 装伪 helm）：

```bash
command -v helm || brew install helm
helm version --short
```

3. 仓库环境门禁：

```bash
bash production/scripts/check_env.sh
# 期望：ok helm=v4.2.3… k8s=Ready
```

4. compose 基座（Kafka / ClickHouse / MinIO）已起；Kafka 须发布 **9095**（K8S listener）：

```bash
cd docker && docker compose up -d kafka clickhouse minio minio-init
# 确认端口：9094（宿主机造数）、9095（K8s 作业）、8123、9000
```

## 安装 Operator

一键脚本（推荐）：

```bash
bash production/scripts/install-operator.sh
```

等价手写命令（坐标与根 README 版本矩阵一致）：

```bash
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm repo update flink-operator-repo

helm upgrade --install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator \
  --create-namespace \
  --set webhook.create=false \
  --set image.tag=1.15.0 \
  --wait --timeout 5m

kubectl get pods -n flink-operator
kubectl get crd flinkbluegreendeployments.flink.apache.org
```

验收：

- `kubectl get crd flinkbluegreendeployments.flink.apache.org` 成功
- `kubectl get pods -n flink-operator` 中 operator 相关 Pod 为 Running

学习工程关闭 webhook（`--set webhook.create=false`），因此**不**安装 cert-manager。若日后开启 webhook，须先在版本矩阵登记 cert-manager 再安装。

## K8s ↔ Kafka / MinIO 探针（锁定 bootstrap）

```bash
bash production/scripts/probe_kafka_from_k8s.sh
```

### 实测结论（OrbStack）

| 探测 | 结果 |
|------|------|
| 普通 Pod → `host.docker.internal:9094` TCP | 通 |
| 普通 Pod → `host.docker.internal:9094` Kafka 协议 | **失败**（EXTERNAL advertised=`localhost:9094`，metadata 回环） |
| 普通 Pod → `host.docker.internal:9095` Kafka 协议 | **成功**（compose `K8S` listener advertised=`host.docker.internal:9095`） |
| 普通 Pod → `host.docker.internal:8123` / `:9000` HTTP | 通（ClickHouse / MinIO） |

**锁定写法（写入 chart `values.yaml`）：**

- Job 参数：`--kafka-bootstrap host.docker.internal:9095`
- ClickHouse：`--clickhouse-url http://host.docker.internal:8123/`
- Checkpoint/Savepoint MinIO：`http://host.docker.internal:9000`（凭据与 `docker/.env` 一致：`flinklab` / `flinklab123`）
- **不**使用 Pod `hostNetwork`（单节点上 JM+TM 会抢占宿主机端口）

**禁止**在 K8s 作业里使用 compose 内网服务名作为 bootstrap（该 DNS 在集群内不可解析）。

## 构建 Application 镜像并部署 chart

```bash
# 1) 打包 p03 shade jar
make -C projects/p03-vehicle-monitoring package

# 2) 本地镜像（非 :latest；OrbStack 可见 + IfNotPresent）
docker build \
  -f production/docker/p03-k8s-image/Dockerfile \
  -t flinklab/p03-vehicle-alert:dev \
  -t flinklab/p03-vehicle-alert:dev-green \
  .

# 3) 命名空间与 SA
kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -
kubectl create serviceaccount flink -n flink --dry-run=client -o yaml | kubectl apply -f -

# 4) 安装 FlinkBlueGreenDeployment
helm upgrade --install p03-vehicle-alert \
  production/charts/p03-vehicle-alert \
  --namespace flink \
  --create-namespace

kubectl get flinkbluegreendeployment -n flink
kubectl get flinkdeployment -n flink -o wide
```

观察状态机：先到 `ACTIVE_BLUE`（或等价 `ACTIVE_*`），子资源为 `FlinkDeployment`。完整 TRANSITION 演练见 `bluegreen-sop.md` 与 `bash production/scripts/run-bluegreen-drill.sh`。

## 卸载 Operator（可选）

```bash
helm uninstall flink-kubernetes-operator -n flink-operator
# CRD 默认保留；彻底清理：
# kubectl delete crd flinkdeployments.flink.apache.org \
#   flinksessionjobs.flink.apache.org \
#   flinkbluegreendeployments.flink.apache.org
```
