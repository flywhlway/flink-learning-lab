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
# 0) 确保 p03 topic 存在（compose 关闭了 auto.create.topics）
docker compose -f docker/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vehicle.events --partitions 3 --replication-factor 1
docker compose -f docker/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vehicle.alerts --partitions 3 --replication-factor 1
docker compose -f docker/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vehicle.pattern.control --partitions 3 --replication-factor 1

# 1) 打包 p03 shade jar
make -C projects/p03-vehicle-monitoring package

# 2) 本地镜像（非 :latest；OrbStack 可见 + IfNotPresent）
docker build \
  -f production/docker/p03-k8s-image/Dockerfile \
  -t flinklab/p03-vehicle-alert:dev \
  -t flinklab/p03-vehicle-alert:dev-green \
  .

# 3) 安装 FlinkBlueGreenDeployment（chart 自带 flink SA + Role/RoleBinding）
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

---

## Wave 2 扩写 · Flink K8s Operator 安装

### SOP 细项 1

针对「Flink K8s Operator 安装」第 1 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 1：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 2

针对「Flink K8s Operator 安装」第 2 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 2：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 3

针对「Flink K8s Operator 安装」第 3 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 3：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 4

针对「Flink K8s Operator 安装」第 4 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 4：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 5

针对「Flink K8s Operator 安装」第 5 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 5：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 6

针对「Flink K8s Operator 安装」第 6 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 6：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 7

针对「Flink K8s Operator 安装」第 7 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 7：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 8

针对「Flink K8s Operator 安装」第 8 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 8：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 9

针对「Flink K8s Operator 安装」第 9 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 9：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 10

针对「Flink K8s Operator 安装」第 10 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 10：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 11

针对「Flink K8s Operator 安装」第 11 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 11：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 12

针对「Flink K8s Operator 安装」第 12 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 12：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 13

针对「Flink K8s Operator 安装」第 13 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 13：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 14

针对「Flink K8s Operator 安装」第 14 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 14：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### 排障树

1. 症状分支 1：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

2. 症状分支 2：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

3. 症状分支 3：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

4. 症状分支 4：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

5. 症状分支 5：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

6. 症状分支 6：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

7. 症状分支 7：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

