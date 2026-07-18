#!/usr/bin/env bash
# 仓库级三块 Grafana 看板门禁（PROD-04 / D-10）：恰好 3 个 JSON + Grafana search 可见。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MON_DIR="${REPO_ROOT}/monitoring"

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASS="${GRAFANA_PASS:-flinklab}"

COUNT="$(ls "${MON_DIR}"/*.json 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${COUNT}" -ne 3 ]]; then
  echo "FAIL: monitoring/*.json 数量=${COUNT}（须恰好 3）" >&2
  exit 1
fi

for f in "${MON_DIR}"/*.json; do
  python3 -c "import json,sys; d=json.load(open(sys.argv[1])); assert d.get('panels'), 'empty panels'" "${f}" \
    || { echo "FAIL: 非法或空 panels: ${f}" >&2; exit 1; }
done

for must in platform-overview.json job-deepdive.json ai-cost.json; do
  if [[ ! -s "${MON_DIR}/${must}" ]]; then
    echo "FAIL: 缺少 ${must}" >&2
    exit 1
  fi
done

# Grafana 启动 + file provider 扫描（约 30s）
for i in 1 2 3 4 5 6 7 8 9 10; do
  if SEARCH_JSON="$(curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
    "${GRAFANA_URL}/api/search?query=flinklab" 2>/dev/null)"; then
    FOUND=0
    for title in flinklab-platform-overview flinklab-job-deepdive flinklab-ai-cost; do
      if printf '%s' "${SEARCH_JSON}" | grep -q "${title}"; then
        FOUND=$((FOUND + 1))
      fi
    done
    if [[ "${FOUND}" -ge 1 ]]; then
      echo "ok monitoring_json=${COUNT} grafana=${GRAFANA_URL} search_hits=${FOUND}/3 titles=flinklab-*"
      exit 0
    fi
  fi
  sleep 5
done

echo "FAIL: Grafana search 未发现任何 flinklab-* 仓库级看板（provider 可能未扫描）" >&2
echo "  确认 compose 已挂载 ../monitoring → /var/lib/grafana/dashboards/repo" >&2
echo "  并存在 provider flinklab-repo；等待 updateIntervalSeconds≈30s 后重试" >&2
exit 1
