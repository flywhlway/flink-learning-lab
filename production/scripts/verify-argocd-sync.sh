#!/usr/bin/env bash
# Argo CD sync 校验（PROD-03 / D-07）。
# Wave 0 RED：无 Application 或未 Synced 时 FAIL。
# GitOps 仅 Argo CD（D-07：单一路径，不并行第二套工具）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
APP_NAME="${APP_NAME:-flink-learning-lab}"
NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "FAIL: kubectl 未安装" >&2
  exit 1
fi

# 优先 CR；无 CRD/无实例则 FAIL
if ! kubectl get crd applications.argoproj.io >/dev/null 2>&1; then
  echo "FAIL: CRD applications.argoproj.io 不存在（Argo CD 未安装）" >&2
  exit 1
fi

if ! kubectl get application -n "${NAMESPACE}" "${APP_NAME}" >/dev/null 2>&1; then
  # 任意 namespace 搜一次，仍无则 FAIL
  if ! kubectl get application -A --no-headers 2>/dev/null | grep -q .; then
    echo "FAIL: 无 Argo CD Application（期望 ${NAMESPACE}/${APP_NAME} 或任意已登记应用）" >&2
    exit 1
  fi
  echo "FAIL: Application ${NAMESPACE}/${APP_NAME} 不存在" >&2
  exit 1
fi

SYNC_STATUS="$(kubectl get application -n "${NAMESPACE}" "${APP_NAME}" \
  -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
HEALTH="$(kubectl get application -n "${NAMESPACE}" "${APP_NAME}" \
  -o jsonpath='{.status.health.status}' 2>/dev/null || true)"

if [[ "${SYNC_STATUS}" != "Synced" ]]; then
  echo "FAIL: Application ${APP_NAME} sync.status=${SYNC_STATUS:-<empty>}（期望 Synced） health=${HEALTH:-<empty>}" >&2
  exit 1
fi

echo "ok argocd app=${APP_NAME} sync=Synced health=${HEALTH:-unknown}"
