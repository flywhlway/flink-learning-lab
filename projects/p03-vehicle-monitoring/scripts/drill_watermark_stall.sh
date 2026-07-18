#!/usr/bin/env bash
# p03 VEH-06 watermark 停滞演练（D-09/D-10/D-11）。
#
# 最终职责（03-02 实现）— 两阶段 stall → recover：
#   1) stall: 造部分 CEP 序列后，以 frozen-eventTime HEARTBEAT 持续 ≥45s
#      （禁止仅依赖 idle 分区；作业 withIdleness(30s) 会排除空闲分区，stall 演示失效）
#      断言：CH MATCH 不变；Prometheus currentEmitEventTimeLag 上升
#   2) recover: 补齐 DTC / 推进时间戳尾心跳，再用 PATTERN_ID=… bash scripts/verify.sh
#      断言：停滞可观察 → 恢复后 MATCH/窗口可继续
#
# Wave 0 骨架：默认 FAIL 非 0；完整演练需 --implemented（留给 03-02）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

IMPLEMENTED=0
for arg in "$@"; do
  case "${arg}" in
    --implemented) IMPLEMENTED=1 ;;
  esac
done

echo "drill: phase=stall frozen-eventTime HEARTBEAT（Wave0 stub；未执行造数）"
echo "drill: phase=recover PATTERN_ID verify.sh（Wave0 stub；未执行）"

if [[ "${IMPLEMENTED}" -ne 1 ]]; then
  echo "FAIL: Wave0 stub — drill_watermark_stall 完整演练尚未实现（stall→recover / frozen-eventTime）" >&2
  exit 1
fi

echo "FAIL: drill --implemented 路径尚未接线 gen/Prom/verify（Wave 0）" >&2
exit 1
