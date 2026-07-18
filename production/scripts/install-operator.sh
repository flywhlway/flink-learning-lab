#!/usr/bin/env bash
# 安装 Flink Kubernetes Operator 1.15.0（Helm，SSOT 见根 README 版本矩阵）。
# D-04：OrbStack K8s；webhook.create=false（免 cert-manager）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

OPERATOR_VERSION="1.15.0"
HELM_REPO_NAME="flink-operator-repo"
HELM_REPO_URL="https://downloads.apache.org/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/"
RELEASE_NAME="flink-kubernetes-operator"
NAMESPACE="flink-operator"
CRD_NAME="flinkbluegreendeployments.flink.apache.org"

bash "${SCRIPT_DIR}/check_env.sh"

echo "==> helm repo add/update ${HELM_REPO_NAME}"
helm repo add "${HELM_REPO_NAME}" "${HELM_REPO_URL}" 2>/dev/null \
  || helm repo add "${HELM_REPO_NAME}" "${HELM_REPO_URL}" --force-update
helm repo update "${HELM_REPO_NAME}" >/dev/null

echo "==> helm upgrade --install ${RELEASE_NAME} (image.tag=${OPERATOR_VERSION}, webhook.create=false)"
helm upgrade --install "${RELEASE_NAME}" "${HELM_REPO_NAME}/flink-kubernetes-operator" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --set webhook.create=false \
  --set image.tag="${OPERATOR_VERSION}" \
  --wait \
  --timeout 5m

echo "==> 等待 Operator Pod Ready"
kubectl wait --for=condition=Ready pod \
  -l app.kubernetes.io/name=flink-kubernetes-operator \
  -n "${NAMESPACE}" \
  --timeout=180s 2>/dev/null \
  || kubectl wait --for=condition=Ready pod \
    -l app.kubernetes.io/instance="${RELEASE_NAME}" \
    -n "${NAMESPACE}" \
    --timeout=180s

if ! kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
  echo "FAIL: CRD ${CRD_NAME} 不存在（Operator 安装未就绪）" >&2
  kubectl get crd | grep -i flink || true
  kubectl get pods -n "${NAMESPACE}" -o wide || true
  exit 1
fi

echo "==> CRD ${CRD_NAME} ok"
kubectl get pods -n "${NAMESPACE}" -o wide
echo "ok operator=${OPERATOR_VERSION} ns=${NAMESPACE}"
