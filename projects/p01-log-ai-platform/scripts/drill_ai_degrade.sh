#!/usr/bin/env bash
# p01 LOG-02/D-14 演练：AI off / 不可用时降级路径仍绿。
#   职责：关闭或模拟 AI 不可用 → 跑规则路径 verify → 期望 exit 0
#   权威出口仍为 ClickHouse（非 Kafka）。
#
# Wave 0 骨架：默认 exit 1。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFY_SH="${SCRIPT_DIR}/verify.sh"

if [[ "${1:-}" != "--implemented" ]]; then
  echo "FAIL: Wave0 stub — drill_ai_degrade.sh（AI degrade 演练）尚未实现" >&2
  echo "hint: 完成后应在 AI 关闭时调用 ${VERIFY_SH} 并期望绿" >&2
  exit 1
fi

echo "FAIL: Wave0 stub — --implemented 路径尚未接线（AI degrade → verify）" >&2
exit 1
