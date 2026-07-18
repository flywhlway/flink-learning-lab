#!/usr/bin/env bash
# p01 LOG-05 / D-13 压测入口（项目级，非 benchmark/ 全矩阵）：
#   造数 --rate/--duration → 刮指标 → 写 docs/baseline.md
#   禁止 k6/JMeter；数字仅 OrbStack arm64 实测后写入。
#   权威出口口径对齐 verify（CH），Kafka 仅诊断。
#
# Wave 0 骨架：默认 exit 1。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
P01_DIR="${REPO_ROOT}/projects/p01-log-ai-platform"
BASELINE_MD="${P01_DIR}/docs/baseline.md"

RATE="${RATE:-100}"
DURATION_SEC="${DURATION_SEC:-120}"

if [[ "${1:-}" != "--implemented" ]]; then
  echo "FAIL: Wave0 stub — loadtest.sh 将写 ${BASELINE_MD}，尚未实现" >&2
  echo "hint: RATE=${RATE} DURATION_SEC=${DURATION_SEC}；完成后: bash scripts/loadtest.sh --implemented" >&2
  exit 1
fi

echo "FAIL: Wave0 stub — --implemented 路径尚未接线（baseline.md）" >&2
exit 1
