# Blue/Green 操作 SOP（p03 VehicleAlertJob）

PROD-02 / D-05 / D-06 / D-09：在 OrbStack K8s 上对 `FlinkBlueGreenDeployment` 做可观察切换与回滚。

**硬门禁**是 Blue/Green CR 状态机（`ACTIVE_BLUE` ↔ `TRANSITIONING_*` ↔ `ACTIVE_GREEN`）+ 脚本时间线，**不是**单独改 `job.upgradeMode`。

## 前置

```bash
bash production/scripts/check_env.sh
bash production/scripts/install-operator.sh
bash production/scripts/probe_kafka_from_k8s.sh
# compose Kafka/ClickHouse/MinIO 已起；topic vehicle.events / vehicle.alerts / vehicle.pattern.control 存在
```

构建镜像（非 `:latest`）：

```bash
make -C projects/p03-vehicle-monitoring package
docker build -f production/docker/p03-k8s-image/Dockerfile \
  -t flinklab/p03-vehicle-alert:dev \
  -t flinklab/p03-vehicle-alert:dev-green .
```

## 一键演练（推荐）

```bash
bash production/scripts/run-bluegreen-drill.sh
# 期望 exit 0；证据：production/docs/bluegreen-timeline.md
```

脚本步骤：部署 chart → 等待 `ACTIVE_*` + 子 `FlinkDeployment` `RUNNING`/`STABLE` → `--set image.tag` 触发 TRANSITION → 轮询对侧 `ACTIVE_*` → 写入事件与 CR 片段。

## 手工分步

### 1) 部署

```bash
helm upgrade --install p03-vehicle-alert production/charts/p03-vehicle-alert \
  --namespace flink --create-namespace \
  --set image.tag=dev

kubectl get flinkbluegreendeployment -n flink -w
# 期望：blueGreenState=ACTIVE_BLUE（首次）
kubectl get flinkdeployment -n flink -o wide
# 期望：JOB STATUS=RUNNING，LIFECYCLE STATE=STABLE
```

### 2) 观察

```bash
kubectl get flinkbluegreendeployment p03-vehicle-alert-bg -n flink -o yaml
kubectl get flinkdeployment -n flink -o wide
kubectl get events -n flink --sort-by='.lastTimestamp' | tail -n 40
```

### 3) 切换（触发 TRANSITION）

镜像 tag 变更属于官方文档中的 TRANSITION diff（非 PATCH_CHILD）：

```bash
helm upgrade --install p03-vehicle-alert production/charts/p03-vehicle-alert \
  --namespace flink --reuse-values \
  --set image.tag=dev-green

kubectl get flinkbluegreendeployment p03-vehicle-alert-bg -n flink \
  -o jsonpath='{.status.blueGreenState}{"\n"}'
# 过程中可见 SAVEPOINTING_* / TRANSITIONING_TO_GREEN（或反向）
# 终态：ACTIVE_GREEN（若从 ACTIVE_BLUE 出发）
```

### 4) 回滚

**再切回**（推荐学习路径，与正向同一状态机）：

```bash
helm upgrade --install p03-vehicle-alert production/charts/p03-vehicle-alert \
  --namespace flink --reuse-values \
  --set image.tag=dev
# 期望：ACTIVE_GREEN → … → ACTIVE_BLUE
```

**中止失败切换**：若新侧在 `abort.grace-period`（chart 默认 `8 min`）内未达 RUNNING+STABLE，控制器 abort 并回到原 `ACTIVE_*`，旧侧继续服务。排查：

```bash
kubectl describe flinkbluegreendeployment p03-vehicle-alert-bg -n flink
kubectl get events -n flink --sort-by='.lastTimestamp' | tail -n 80
kubectl logs -n flink-operator -l app.kubernetes.io/name=flink-kubernetes-operator --tail=100
```

## 附录 A：三种 upgradeMode 对照（非 BG 硬门禁）

| upgradeMode | 含义 | 与 Blue/Green |
|-------------|------|----------------|
| `stateless` | 丢状态重启 | 可作对照实验 |
| `savepoint` | 切换前触发 savepoint（本 chart 默认） | 与 BG SAVEPOINTING_* 协同 |
| `last-state` | 依赖 HA 元数据恢复 | 需 HA ConfigMap/存储 |

单独改 `upgradeMode` **不等于**完成了 Blue/Green 演练。

## 附录 B：Session / Autoscaler（可选，非硬门禁）

Session 集群与 Kubernetes Autoscaler 可在 Operator 文档路径下另开实验；PROD-02 验收不依赖它们。本 SOP 与 `run-bluegreen-drill.sh` 不以 Session/Autoscaler 为 exit 0 条件。
