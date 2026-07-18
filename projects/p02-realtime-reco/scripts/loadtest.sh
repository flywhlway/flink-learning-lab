#!/usr/bin/env bash
# p02 RECO-03 / D-11 压测入口（项目级，非 benchmark/ 全矩阵）：
#   gen --rate/--duration → 刮 Prometheus / ClickHouse → 写 docs/baseline.md
# 禁止 k6/JMeter；权威观测仍以 CH reco_results 为准；Kafka/Redis 仅诊断。
# 数字仅 OrbStack arm64 实测；禁止编造。
#
# Wave 0 骨架：默认失败态；传入 --implemented 后才进入完整压测（留给 05-03）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
P02_DIR="${REPO_ROOT}/projects/p02-realtime-reco"
BASELINE_MD="${P02_DIR}/docs/baseline.md"

RATE="${RATE:-100}"
WARMUP_SEC="${WARMUP_SEC:-30}"
DURATION_SEC="${DURATION_SEC:-90}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"

IMPLEMENTED=0
for arg in "$@"; do
  case "${arg}" in
    --implemented) IMPLEMENTED=1 ;;
  esac
done

if [[ "${IMPLEMENTED}" -ne 1 ]]; then
  echo "FAIL: Wave0 stub — loadtest 尚未实现完整绿路径" >&2
  echo "hint: 将写入 ${BASELINE_MD}；默认 RATE=${RATE} WARMUP=${WARMUP_SEC}s DURATION=${DURATION_SEC}s bootstrap=${BOOTSTRAP}" >&2
  echo "hint: 传入 --implemented 启用完整压测（05-03）" >&2
  exit 1
fi

echo "p02 loadtest: rate=${RATE} warmup=${WARMUP_SEC}s measure=${DURATION_SEC}s → ${BASELINE_MD}"
echo "FAIL: Wave0 stub — gen/scrape/baseline 写入尚未接线" >&2
exit 1
