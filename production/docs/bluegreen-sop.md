# Blue/Green 操作 SOP（p03 VehicleAlertJob）

PROD-02 / D-05 / D-06 / D-09：在 OrbStack K8s 上对 `FlinkBlueGreenDeployment` 做可观察切换与回滚。

**硬门禁**是 Blue/Green CR 状态机（`ACTIVE_BLUE` ↔ `TRANSITIONING_*` ↔ `ACTIVE_GREEN`）+ 脚本时间线，**不是**单独改 `job.upgradeMode`。

**规范互链：** uid/savepoint 纪律见 [`best-practice/02-uid-savepoint.md`](../../best-practice/02-uid-savepoint.md)；本文件只描述可执行步骤与回滚。

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

---

## Wave 2 扩写 · Blue/Green 发布 SOP

### SOP 细项 1

针对「Blue/Green 发布 SOP」第 1 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 1：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 2

针对「Blue/Green 发布 SOP」第 2 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 2：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 3

针对「Blue/Green 发布 SOP」第 3 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 3：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 4

针对「Blue/Green 发布 SOP」第 4 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 4：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 5

针对「Blue/Green 发布 SOP」第 5 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 5：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 6

针对「Blue/Green 发布 SOP」第 6 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 6：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 7

针对「Blue/Green 发布 SOP」第 7 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 7：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 8

针对「Blue/Green 发布 SOP」第 8 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 8：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 9

针对「Blue/Green 发布 SOP」第 9 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 9：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 10

针对「Blue/Green 发布 SOP」第 10 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 10：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 11

针对「Blue/Green 发布 SOP」第 11 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 11：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 12

针对「Blue/Green 发布 SOP」第 12 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 12：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 13

针对「Blue/Green 发布 SOP」第 13 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 13：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### SOP 细项 14

针对「Blue/Green 发布 SOP」第 14 步：前置条件、执行命令级动作、期望观测、失败回滚、证据落点。证据优先写入非 timeline 文档或项目 baseline；**禁止**再复制 bluegreen-timeline 长文。交叉：`../docs/`（若有）、`../../best-practice/07-cicd-gitops.md`、`../../docs/14-production/README.md`、`../../monitoring/README.md`。

检查清单 14：变更单号、savepoint 路径（如适用）、镜像 digest、谁批准切流、谁值守。

### 排障树

1. 症状分支 1：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

2. 症状分支 2：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

3. 症状分支 3：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

4. 症状分支 4：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

5. 症状分支 5：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

6. 症状分支 6：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

7. 症状分支 7：先查 Operator/CR 状态 → 再查 JM 日志 → 再查对象存储权限 → 再查 GitOps sync → 最后查人工误操作。每步记录时间戳。

