#!/usr/bin/env bash
# Blue/Green 可观察演练（PROD-02 / D-06）。
# 部署 p03 FlinkBlueGreenDeployment → 等待 ACTIVE_* + RUNNING →
# 改 image tag 触发 TRANSITION → 轮询对侧 ACTIVE_* → 写出时间线证据。
# 失败非 0。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

OUT="${1:-${REPO_ROOT}/production/docs/bluegreen-timeline.md}"
NS="${BG_NAMESPACE:-flink}"
RELEASE="${BG_RELEASE:-p03-vehicle-alert}"
CR_NAME="${BG_CR_NAME:-p03-vehicle-alert-bg}"
CHART_DIR="${REPO_ROOT}/production/charts/p03-vehicle-alert"
CRD_NAME="flinkbluegreendeployments.flink.apache.org"
BLUE_TAG="${BG_BLUE_TAG:-dev}"
GREEN_TAG="${BG_GREEN_TAG:-dev-green}"
WAIT_ACTIVE_SEC="${BG_WAIT_ACTIVE_SEC:-300}"
WAIT_TRANSITION_SEC="${BG_WAIT_TRANSITION_SEC:-600}"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

log() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

append_section() {
  local title="$1"
  {
    echo
    echo "## ${title}"
    echo
    echo "UTC: $(utc_now)"
    echo
    echo '```'
    cat
    echo '```'
  } >>"${OUT}"
}

bg_state() {
  kubectl get flinkbluegreendeployment "${CR_NAME}" -n "${NS}" \
    -o jsonpath='{.status.blueGreenState}' 2>/dev/null || echo ""
}

active_job_state() {
  kubectl get flinkdeployment -n "${NS}" \
    -l "app.kubernetes.io/name!=dummy" \
    -o jsonpath='{range .items[*]}{.metadata.name}={.status.jobStatus.state}/{.status.lifecycleState}{"\n"}{end}' \
    2>/dev/null || true
}

ensure_topics() {
  log "确保 compose Kafka topic 存在（vehicle.*）"
  if ! docker compose -f "${COMPOSE_FILE}" ps 2>/dev/null | grep -E 'fll-kafka.*running|fll-kafka.*Up' >/dev/null \
    && ! docker ps --format '{{.Names}} {{.Status}}' 2>/dev/null | grep -q '^fll-kafka .*Up'; then
    fail "compose Kafka（fll-kafka）未运行；请先 docker compose up -d kafka"
  fi
  local t
  for t in vehicle.events vehicle.alerts vehicle.pattern.control; do
    docker compose -f "${COMPOSE_FILE}" exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "${t}" --partitions 3 --replication-factor 1 \
      >/dev/null
  done
}

wait_for_active() {
  local deadline=$(( $(date +%s) + WAIT_ACTIVE_SEC ))
  local st job
  while true; do
    st="$(bg_state)"
    job="$(kubectl get flinkdeployment -n "${NS}" -o jsonpath='{.items[0].status.jobStatus.state}' 2>/dev/null || echo "")"
    log "等待 ACTIVE_* + RUNNING：blueGreenState=${st} job=${job}"
    if [[ "${st}" == "ACTIVE_BLUE" || "${st}" == "ACTIVE_GREEN" ]] && [[ "${job}" == "RUNNING" ]]; then
      return 0
    fi
    if [[ "$(date +%s)" -ge "${deadline}" ]]; then
      fail "超时 ${WAIT_ACTIVE_SEC}s 未达到 ACTIVE_* + RUNNING（state=${st} job=${job}）"
    fi
    sleep 10
  done
}

wait_for_transition_to() {
  local target="$1"
  local deadline=$(( $(date +%s) + WAIT_TRANSITION_SEC ))
  local st saw_transition=0
  while true; do
    st="$(bg_state)"
    log "TRANSITION 轮询：blueGreenState=${st}（目标 ${target}）"
    if [[ "${st}" == TRANSITIONING_* || "${st}" == SAVEPOINTING_* ]]; then
      saw_transition=1
      {
        echo
        echo "### Snapshot during ${st}"
        echo
        echo "UTC: $(utc_now)"
        echo
        echo '```yaml'
        kubectl get flinkbluegreendeployment "${CR_NAME}" -n "${NS}" -o yaml 2>/dev/null | head -120 || true
        echo '```'
        echo
        echo '```'
        kubectl get flinkdeployment -n "${NS}" -o wide 2>/dev/null || true
        echo '```'
      } >>"${OUT}"
    fi
    if [[ "${st}" == "${target}" ]]; then
      [[ "${saw_transition}" -eq 1 ]] || log "WARN: 未捕获中间 TRANSITIONING_*（可能过快）；终态 ${target} 已达"
      return 0
    fi
    if [[ "${st}" == "FAILING" || "${st}" == "FAILED" ]]; then
      fail "Blue/Green 进入 ${st}；见 ${OUT} 与 kubectl describe"
    fi
    if [[ "$(date +%s)" -ge "${deadline}" ]]; then
      fail "超时 ${WAIT_TRANSITION_SEC}s 未到达 ${target}（当前 ${st}）"
    fi
    sleep 10
  done
}

# ── 门禁 ──
command -v kubectl >/dev/null || fail "kubectl 未安装"
command -v helm >/dev/null || fail "helm 未安装"
bash "${SCRIPT_DIR}/check_env.sh"
kubectl get crd "${CRD_NAME}" >/dev/null 2>&1 || fail "CRD ${CRD_NAME} 不存在；先 bash production/scripts/install-operator.sh"
bash "${SCRIPT_DIR}/probe_kafka_from_k8s.sh"
ensure_topics

# 镜像（蓝/绿同内容不同 tag，触发 TRANSITION）
if ! docker image inspect "flinklab/p03-vehicle-alert:${BLUE_TAG}" >/dev/null 2>&1; then
  log "构建本地镜像 flinklab/p03-vehicle-alert:${BLUE_TAG}"
  test -f projects/p03-vehicle-monitoring/target/p03-vehicle-monitoring-0.1.0.jar \
    || fail "缺少 p03 shade jar；先 make -C projects/p03-vehicle-monitoring package"
  docker build -f production/docker/p03-k8s-image/Dockerfile \
    -t "flinklab/p03-vehicle-alert:${BLUE_TAG}" \
    -t "flinklab/p03-vehicle-alert:${GREEN_TAG}" .
fi
docker tag "flinklab/p03-vehicle-alert:${BLUE_TAG}" "flinklab/p03-vehicle-alert:${GREEN_TAG}" 2>/dev/null || true

mkdir -p "$(dirname "${OUT}")"
{
  echo "# Blue/Green Timeline"
  echo
  echo "PROD-02 / D-06 可观察演练证据（脚本 \`production/scripts/run-bluegreen-drill.sh\` 生成）。"
  echo
  echo "- Started UTC: $(utc_now)"
  echo "- Namespace: \`${NS}\`"
  echo "- CR: \`${CR_NAME}\`"
  echo "- Blue image tag: \`${BLUE_TAG}\` → Green image tag: \`${GREEN_TAG}\`"
  echo
} >"${OUT}"

kubectl get crd "${CRD_NAME}" -o wide 2>/dev/null | append_section "CRD"
kubectl get nodes -o wide 2>/dev/null | append_section "Nodes"

log "部署 / 重置 chart（image.tag=${BLUE_TAG}）"
helm upgrade --install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NS}" \
  --create-namespace \
  --set "image.tag=${BLUE_TAG}" \
  --wait=hookOnly

wait_for_active
START_STATE="$(bg_state)"
log "稳态已达：${START_STATE}"

{
  echo
  echo "## Pre-transition status"
  echo
  echo "UTC: $(utc_now)"
  echo
  echo "### FlinkBlueGreenDeployment status"
  echo
  echo '```yaml'
  kubectl get flinkbluegreendeployment "${CR_NAME}" -n "${NS}" -o yaml
  echo '```'
  echo
  echo "### Child FlinkDeployments"
  echo
  echo '```'
  kubectl get flinkdeployment -n "${NS}" -o wide
  echo '```'
  echo
  echo '```yaml'
  kubectl get flinkdeployment -n "${NS}" -o yaml | head -160
  echo '```'
  echo
  echo "### Events"
  echo
  echo '```'
  kubectl get events -n "${NS}" --sort-by='.lastTimestamp' | tail -n 80
  echo '```'
} >>"${OUT}"

if [[ "${START_STATE}" == "ACTIVE_BLUE" ]]; then
  TARGET_STATE="ACTIVE_GREEN"
  TRIGGER_TAG="${GREEN_TAG}"
elif [[ "${START_STATE}" == "ACTIVE_GREEN" ]]; then
  TARGET_STATE="ACTIVE_BLUE"
  TRIGGER_TAG="${BLUE_TAG}"
else
  fail "非预期稳态 ${START_STATE}"
fi

log "触发 TRANSITION：helm --set image.tag=${TRIGGER_TAG}（期望 ${START_STATE} → ${TARGET_STATE}）"
{
  echo
  echo "## Trigger TRANSITION"
  echo
  echo "UTC: $(utc_now)"
  echo
  echo "- From: \`${START_STATE}\`"
  echo "- To (expected): \`${TARGET_STATE}\`"
  echo "- Command: \`helm upgrade --install ${RELEASE} ... --set image.tag=${TRIGGER_TAG}\`"
  echo
} >>"${OUT}"

helm upgrade --install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NS}" \
  --reuse-values \
  --set "image.tag=${TRIGGER_TAG}" \
  --wait=hookOnly

wait_for_transition_to "${TARGET_STATE}"
END_STATE="$(bg_state)"
log "TRANSITION 完成：${START_STATE} → ${END_STATE}"

{
  echo
  echo "## Post-transition status"
  echo
  echo "UTC: $(utc_now)"
  echo
  echo "- Observed migration: \`${START_STATE}\` → \`${END_STATE}\`"
  echo
  echo "### FlinkBlueGreenDeployment status"
  echo
  echo '```yaml'
  kubectl get flinkbluegreendeployment "${CR_NAME}" -n "${NS}" -o yaml
  echo '```'
  echo
  echo "### Child FlinkDeployments"
  echo
  echo '```'
  kubectl get flinkdeployment -n "${NS}" -o wide
  echo '```'
  echo
  echo '```yaml'
  kubectl get flinkdeployment -n "${NS}" -o yaml | head -160
  echo '```'
  echo
  echo "### Events"
  echo
  echo '```'
  kubectl get events -n "${NS}" --sort-by='.lastTimestamp' | tail -n 80
  echo '```'
  echo
  echo "## Result"
  echo
  echo "- Finished UTC: $(utc_now)"
  echo "- Outcome: **PASS** (\`${START_STATE}\` → \`${END_STATE}\`)"
  echo
} >>"${OUT}"

test -s "${OUT}" || fail "时间线文件为空：${OUT}"
rg -n "ACTIVE_|FlinkDeployment|Events|UTC" "${OUT}" >/dev/null \
  || fail "时间线缺少必备字段（ACTIVE_/FlinkDeployment/Events/UTC）"

log "ok drill → ${OUT}"
exit 0
