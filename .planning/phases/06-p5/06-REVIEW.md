---
phase: 06-p5
reviewed: 2026-07-18T06:27:00Z
depth: standard
files_reviewed: 28
files_reviewed_list:
  - .github/workflows/ci.yml
  - benchmark/Makefile
  - benchmark/scripts/collect_metrics.py
  - benchmark/scripts/run_matrix.sh
  - docker/config/grafana/provisioning/dashboards/dashboards.yml
  - docker/docker-compose.yml
  - examples/e01-hello-flink/src/main/java/com/flywhl/flinklab/e01/KafkaClickstreamWindowJob.java
  - examples/e10-cep/src/main/java/com/flywhl/flinklab/e10/C5VehicleDtcPatternJob.java
  - monitoring/ai-cost.json
  - monitoring/job-deepdive.json
  - monitoring/platform-overview.json
  - monitoring/scripts/verify_repo_dashboards.sh
  - production/argocd/application-p03.yaml
  - production/charts/p03-vehicle-alert/Chart.yaml
  - production/charts/p03-vehicle-alert/templates/flink-bluegreen.yaml
  - production/charts/p03-vehicle-alert/templates/rbac.yaml
  - production/charts/p03-vehicle-alert/values.yaml
  - production/docker/p03-k8s-image/Dockerfile
  - production/scripts/check_env.sh
  - production/scripts/install-argocd.sh
  - production/scripts/install-operator.sh
  - production/scripts/probe_kafka_from_k8s.sh
  - production/scripts/run-bluegreen-drill.sh
  - production/scripts/verify-argocd-sync.sh
  - scripts/count_interview.py
  - scripts/qa_check.sh
  - monitoring/README.md
  - benchmark/baseline.md
findings:
  critical: 1
  warning: 4
  info: 3
  total: 8
status: issues_found
---

# Phase 6: Code Review Report

**Reviewed:** 2026-07-18T06:27:00Z  
**Depth:** standard  
**Files Reviewed:** 28  
**Status:** issues_found

## Summary

审查范围来自全部 `06-*-SUMMARY.md` 的 `key-files`（created/modified），过滤规划产物后聚焦生产脚本、Helm/Argo、压测 harness、CI、Grafana JSON 与相关 Java 旋钮。interview / best-practice / docs 正文仅做违禁词扫描（无 TODO/FIXME/「请参考官网」命中），未纳入深度逻辑审查。

核心问题：`collect_metrics.py` 仍查询已废弃的反压 series 名，导致权威 `benchmark/baseline.md` 九单元格 **backpressure 列全部为 `n/a`**，与 Phase 06-04 已修正的 `monitoring/` 值班指标不一致——PROD-01「值班五指标」采集不完整。其余为门禁偏松、BG 轮询歧义、CI 未编 p03、以及学习工程明文凭据等可改进项。

## Critical Issues

### CR-01: 反压指标 series 名错误，baseline 系统性缺失

**File:** `benchmark/scripts/collect_metrics.py:19`  
**Issue:** `DEFAULT_QUERIES["backpressure_ms_per_s"]` 使用 `flink_taskmanager_job_task_isBackPressuredTimeMsPerSecond`。Phase 06-04 已确认本机 Prometheus 真实序列为 `flink_taskmanager_job_task_backPressuredTimeMsPerSecond`（见 `monitoring/README.md` / `platform-overview.json`），旧名无数据。`run_matrix.sh` 的刮取成功判据不要求 backpressure 非空，矩阵仍 exit 0，但权威报告九行反压均为 `n/a`（见 `benchmark/baseline.md:39-47`）。  
**Fix:**
```python
DEFAULT_QUERIES = {
    "backpressure_ms_per_s": "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
    "busy_ms_per_s": "flink_taskmanager_job_task_busyTimeMsPerSecond",
    "last_checkpoint_duration": "flink_jobmanager_job_lastCheckpointDuration",
    "num_restarts": "flink_jobmanager_job_numRestarts",
    "emit_event_time_lag": "flink_taskmanager_job_task_operator_currentEmitEventTimeLag",
}
```
修正后重跑 `make -C benchmark matrix`（或至少复刮验证非空）再更新 `baseline.md`。

## Warnings

### WR-01: Grafana 看板门禁在仅命中 1/3 时仍放行

**File:** `monitoring/scripts/verify_repo_dashboards.sh:41-44`  
**Issue:** 循环内 `if [[ "${FOUND}" -ge 1 ]]` 即 exit 0，未要求三个 title（`flinklab-platform-overview` / `flinklab-job-deepdive` / `flinklab-ai-cost`）全部出现。PROD-04 / D-10「恰好三块可见」可被假绿。  
**Fix:**
```bash
if [[ "${FOUND}" -eq 3 ]]; then
  echo "ok monitoring_json=${COUNT} grafana=${GRAFANA_URL} search_hits=${FOUND}/3 titles=flinklab-*"
  exit 0
fi
```

### WR-02: Blue/Green 初态等待用 `items[0]`，多 CR 时可能误判

**File:** `production/scripts/run-bluegreen-drill.sh:75`  
**Issue:** `wait_for_active` 取 `flinkdeployment` 列表的 `{.items[0].status.jobStatus.state}`，K8s 列表顺序不确定；演练中若存在旧子部署或其它 FlinkDeployment，可能把非目标 job 状态当成 RUNNING，或漏掉真实作业未就绪。  
**Fix:** 按 label / 名称过滤（例如 `app.kubernetes.io/name=p03-vehicle-alert` 或 CR 关联的 blue/green 子名），或 `jsonpath` 遍历确认「至少一个 RUNNING」且属于本 release：
```bash
job="$(kubectl get flinkdeployment -n "${NS}" -l app.kubernetes.io/name=p03-vehicle-alert \
  -o jsonpath='{range .items[*]}{.status.jobStatus.state}{"\n"}{end}' \
  | grep -c '^RUNNING$' || true)"
[[ "${job}" -ge 1 ]]
```

### WR-03: p03 压测单元格 emitEventTimeLag 被 earliest + 新 group 污染

**File:** `benchmark/scripts/run_matrix.sh:310-318`（交互：`projects/.../VehicleAlertJob.java:67`）  
**Issue:** 矩阵为每单元格使用新 `--group-id p03-bench-${cell_id}`，而 `VehicleAlertJob` 固定 `OffsetsInitializer.earliest()`。新消费组会重放 topic 历史，baseline 中 p03 两行 lag ≈ `9.7e6` ms，与 e01/e10 量级脱节，不能代表计量窗口内延迟。  
**Fix（任选其一，需 OrbStack 复测）：**
- 作业增加 `--starting-offsets latest|earliest`（bench 传 `latest`）；或
- harness 在计量前清空/专用 bench topic，并保证无历史积压；或
- p03 单元格对 lag 列标记 `SKIPPED+原因`（禁止把重放 lag 当稳态）。

### WR-04: CI required 路径未编译 `projects/p03`（及生产镜像依赖 jar）

**File:** `.github/workflows/ci.yml:40-42`  
**Issue:** required step 仅 `examples` 下 `mvn compile`；`qa_check.sh` 的 mvn 同样只编 examples 且失败仅 warn。P5 核心交付物（p03 chart / K8s 镜像 / BG 演练）依赖 `projects/p03-vehicle-monitoring` shade jar，CI 无法捕获 p03 编译回归。  
**Fix:**
```yaml
- name: Maven compile（examples + p03）
  run: |
    (cd examples && mvn -B -T1C -DskipTests compile)
    (cd projects/p03-vehicle-monitoring && mvn -B -T1C -DskipTests package)
```

## Info

### IN-01: Chart values 明文写入学习工程凭据

**File:** `production/charts/p03-vehicle-alert/values.yaml:26-29,55-60`  
**Issue:** ClickHouse / MinIO 账号密码以明文进入 Helm values（最终进 CR `flinkConfiguration`）。与 compose 演示凭据一致，适合本仓库学习场景，但勿复用到真实集群。  
**Fix:** 文档已部分覆盖；可选改为 `secretKeyRef` / `--set-file`，并确保 `production/docs/*` 标明「仅 OrbStack 演示」。

### IN-02: Argo CD `server.insecure=true` + git-daemon `insecure` 仓库

**File:** `production/scripts/install-argocd.sh:92,169`  
**Issue:** 本地 OrbStack 为简化 TLS 关闭安全项；符合学习工程，但脚本注释应持续强调禁止照搬生产。  
**Fix:** 保持现状即可；若加固，改用 OrbStack 自签证书或 port-forward + HTTPS。

### IN-03: `VehicleAlertJob` 对 hashmap 仍打开 incremental checkpoint

**File:** `projects/p03-vehicle-monitoring/.../VehicleAlertJob.java:50-52`（矩阵通过 `--state-backend` 调用）  
**Issue:** `execution.checkpointing.incremental=true` 无条件设置；HashMap backend 下该选项无意义，可能产生噪音配置。e01/e10 仅在 rocksdb 时开启，更干净。  
**Fix:**
```java
Map<String, String> m = new HashMap<>();
m.put("state.backend.type", cfg.stateBackendType);
if ("rocksdb".equalsIgnoreCase(cfg.stateBackendType)) {
    m.put("execution.checkpointing.incremental", "true");
}
Configuration conf = Configuration.fromMap(m);
```

---

_Reviewed: 2026-07-18T06:27:00Z_  
_Reviewer: Claude (gsd-code-reviewer)_  
_Depth: standard_  
_Advisory only — does not block phase completion._
