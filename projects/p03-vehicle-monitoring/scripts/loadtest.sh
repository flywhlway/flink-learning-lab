#!/usr/bin/env bash
# p03 VEH-06 压测入口（D-06/D-07/D-11）：扩展 gen --rate/--duration → 刮 Prometheus → 写 docs/baseline.md。
#
# 最终职责（03-02 实现）：
#   1) 调用 gen_vehicle_events.py --rate/--duration（禁止引入 k6/JMeter）
#   2) curl Prometheus :9090 刮取 p03 吞吐/lag/checkpoint 摘要
#   3) 将环境快照 + 负载定义 + 指标表写入 projects/p03-vehicle-monitoring/docs/baseline.md
#
# Wave 0 骨架：默认 FAIL 非 0；完整压测需 --implemented（留给 03-02）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BASELINE_MD="${REPO_ROOT}/projects/p03-vehicle-monitoring/docs/baseline.md"
PROM_URL="${PROM_URL:-http://localhost:9090}"

IMPLEMENTED=0
for arg in "$@"; do
  case "${arg}" in
    --implemented) IMPLEMENTED=1 ;;
  esac
done

if [[ "${IMPLEMENTED}" -ne 1 ]]; then
  echo "FAIL: Wave0 stub — loadtest 完整压测尚未实现（将写 ${BASELINE_MD}；传 --implemented 留给 03-02）" >&2
  exit 1
fi

# 依赖探测：Prometheus 不可达亦 exit 1（D-11）
if ! curl -sf "${PROM_URL}/-/ready" >/dev/null 2>&1 \
  && ! curl -sf "${PROM_URL}/api/v1/status/config" >/dev/null 2>&1; then
  echo "FAIL: Prometheus 不可达（${PROM_URL}）" >&2
  exit 1
fi

echo "FAIL: loadtest --implemented 路径尚未接线 gen/Prom/baseline（Wave 0）" >&2
exit 1
