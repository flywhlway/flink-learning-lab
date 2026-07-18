#!/usr/bin/env bash
# Argo CD sync 校验（PROD-03 / D-07）。
# 门禁：Application 存在且 sync=Synced；health 为 Healthy，
# 或学习工程等价判据 Progressing（Flink CR 无内置 health.lua 时短暂调谐）。
# GitOps 仅 Argo CD（D-07：单一路径）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
APP_NAME="${APP_NAME:-flinklab-p03-bg}"
NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
# 可接受的 health：Healthy 为主；Progressing 为 Flink CR 调谐等价判据
ACCEPT_HEALTH_REGEX="${ACCEPT_HEALTH_REGEX:-^(Healthy|Progressing)$}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "FAIL: kubectl 未安装" >&2
  exit 1
fi

if ! kubectl get crd applications.argoproj.io >/dev/null 2>&1; then
  echo "FAIL: CRD applications.argoproj.io 不存在（Argo CD 未安装）" >&2
  exit 1
fi

if ! kubectl get application -n "${NAMESPACE}" "${APP_NAME}" >/dev/null 2>&1; then
  if ! kubectl get application -A --no-headers 2>/dev/null | grep -q .; then
    echo "FAIL: 无 Argo CD Application（期望 ${NAMESPACE}/${APP_NAME}）" >&2
    exit 1
  fi
  echo "FAIL: Application ${NAMESPACE}/${APP_NAME} 不存在" >&2
  kubectl get application -A >&2 || true
  exit 1
fi

# 确认 Application 指向 p03 chart path
APP_PATH="$(kubectl get application -n "${NAMESPACE}" "${APP_NAME}" \
  -o jsonpath='{.spec.source.path}' 2>/dev/null || true)"
if [[ "${APP_PATH}" != *"p03-vehicle-alert"* && "${APP_PATH}" != *"production/"* ]]; then
  echo "FAIL: Application path=${APP_PATH:-<empty>} 未指向 production/charts/p03-vehicle-alert" >&2
  exit 1
fi

SYNC_STATUS="$(kubectl get application -n "${NAMESPACE}" "${APP_NAME}" \
  -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
HEALTH="$(kubectl get application -n "${NAMESPACE}" "${APP_NAME}" \
  -o jsonpath='{.status.health.status}' 2>/dev/null || true)"
REPO_URL="$(kubectl get application -n "${NAMESPACE}" "${APP_NAME}" \
  -o jsonpath='{.spec.source.repoURL}' 2>/dev/null || true)"

if [[ "${SYNC_STATUS}" != "Synced" ]]; then
  echo "FAIL: Application ${APP_NAME} sync.status=${SYNC_STATUS:-<empty>}（期望 Synced） health=${HEALTH:-<empty>}" >&2
  kubectl get application -n "${NAMESPACE}" "${APP_NAME}" -o wide >&2 || true
  exit 1
fi

if [[ ! "${HEALTH}" =~ ${ACCEPT_HEALTH_REGEX} ]]; then
  echo "FAIL: Application ${APP_NAME} health=${HEALTH:-<empty>}（期望 Healthy 或 Progressing） sync=${SYNC_STATUS}" >&2
  exit 1
fi

# 目的 ns 应能看到 BG CR（GitOps 落地证据）
if ! kubectl get flinkbluegreendeployment -n flink -o name 2>/dev/null | grep -q .; then
  echo "FAIL: destination ns flink 无 FlinkBlueGreenDeployment（Argo sync 未落地 chart）" >&2
  exit 1
fi

echo "ok sync=${SYNC_STATUS} health=${HEALTH} app=${APP_NAME} path=${APP_PATH} repoURL=${REPO_URL}"
