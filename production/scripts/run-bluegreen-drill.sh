#!/usr/bin/env bash
# Blue/Green 演练入口（PROD-02 / D-06）。
# Wave 0 RED：CRD flinkbluegreendeployments.flink.apache.org 不存在则 FAIL。
# 存在时本 Wave 仅骨架采集 kubectl 状态，不要求演练成功。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT="${1:-${REPO_ROOT}/production/docs/bluegreen-timeline.md}"
CRD_NAME="flinkbluegreendeployments.flink.apache.org"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "FAIL: kubectl 未安装" >&2
  exit 1
fi

if ! kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
  echo "FAIL: CRD ${CRD_NAME} 不存在（Operator 未安装或版本不含 Blue/Green）" >&2
  echo "FAIL: 请先按 production/README 与版本矩阵安装 Flink Kubernetes Operator 1.15.0" >&2
  exit 1
fi

echo "==> CRD ${CRD_NAME} 存在；采集状态骨架（Wave 0 不要求 TRANSITION 成功）"
mkdir -p "$(dirname "${OUT}")"
{
  echo "# Blue/Green Timeline"
  echo "UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "## CRD"
  kubectl get crd "${CRD_NAME}" -o wide
  echo
  echo "## flinkbluegreendeployment"
  kubectl get flinkbluegreendeployment -A -o wide 2>/dev/null || echo "(none)"
  echo
  echo "## Child FlinkDeployments"
  kubectl get flinkdeployment -A -o wide 2>/dev/null || echo "(none)"
} | tee "${OUT}"

# Wave 0：有 CRD 但无完整演练证据时仍 FAIL，避免假绿
if ! kubectl get flinkbluegreendeployment -A --no-headers 2>/dev/null | grep -q .; then
  echo "FAIL: 无 FlinkBlueGreenDeployment 实例；演练未完成（见 D-05/D-06）" >&2
  exit 1
fi

echo "FAIL: Blue/Green 演练时间线尚未完成可观察 TRANSITION（Wave 0 骨架）" >&2
exit 1
