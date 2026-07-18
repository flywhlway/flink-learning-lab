#!/usr/bin/env bash
# p02 RECO-03 / D-12 演练 #1：Redis 不可用时作业凭 Keyed State 仍产出 CH 行。
#
# 最终职责（实现后）：
#   1) stop/pause fll-redis（或断网）
#   2) 造数 → 轮询 verify.sh FEATURE_SOURCE=STATE_ONLY
#   3) 期望 ClickHouse flinklab.reco_results 有行（权威出口，非 Kafka/Redis KEYS）
#
# Wave 0 骨架：默认失败态；传入 --implemented 后才进入完整演练（留给 05-03）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"
VERIFY_SH="${SCRIPT_DIR}/verify.sh"

IMPLEMENTED=0
for arg in "$@"; do
  case "${arg}" in
    --implemented) IMPLEMENTED=1 ;;
  esac
done

if [[ "${IMPLEMENTED}" -ne 1 ]]; then
  echo "FAIL: Wave0 stub — drill_redis_degrade 尚未实现完整绿路径" >&2
  echo "hint: Redis 降级演练将断言 feature_source=STATE_ONLY 的 reco_results 行；请用 --implemented 启用（05-03）" >&2
  exit 1
fi

# --- 以下为实现期占位结构（Wave 0 默认不进入）---
echo "p02 drill_redis_degrade: stop redis → STATE_ONLY → verify CH reco_results"
if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi
if [[ ! -x "${VERIFY_SH}" && ! -f "${VERIFY_SH}" ]]; then
  echo "FAIL: 未找到 ${VERIFY_SH}" >&2
  exit 1
fi

echo "FAIL: Wave0 stub — Redis 停服 / 造数 / FEATURE_SOURCE=STATE_ONLY 轮询尚未接线" >&2
exit 1
