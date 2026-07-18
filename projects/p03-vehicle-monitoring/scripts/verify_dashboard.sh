#!/usr/bin/env bash
# p03 VEH-05 大盘门禁（D-15）：dashboard JSON 非空 + Grafana HTTP 健康。
# ClickHouse 仍是 CEP 告警权威出口（verify.sh）；本脚本只断言大盘可导入/可发现。
#
# 环境变量（可选）：
#   GRAFANA_URL   默认 http://localhost:3000（禁止任意远程默认）
#   GRAFANA_USER  默认 admin
#   GRAFANA_PASS  默认 flinklab（与 compose GF_SECURITY_ADMIN_PASSWORD 对齐）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DASHBOARD_JSON="${REPO_ROOT}/projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json"

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASS="${GRAFANA_PASS:-flinklab}"

if [[ ! -s "${DASHBOARD_JSON}" ]]; then
  echo "FAIL: 缺少非空 dashboard JSON: ${DASHBOARD_JSON}" >&2
  exit 1
fi

if ! curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  "${GRAFANA_URL}/api/datasources" >/dev/null; then
  echo "FAIL: Grafana /api/datasources 不可达或认证失败（${GRAFANA_URL}）" >&2
  exit 1
fi

SEARCH_JSON="$(curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  "${GRAFANA_URL}/api/search?query=p03")" || {
  echo "FAIL: Grafana /api/search?query=p03 失败（${GRAFANA_URL}）" >&2
  exit 1
}

if ! printf '%s' "${SEARCH_JSON}" | grep -q 'p03-vehicle'; then
  echo "FAIL: Grafana search 未发现 p03-vehicle 大盘（query=p03）" >&2
  exit 1
fi

echo "ok dashboard_json=${DASHBOARD_JSON} grafana=${GRAFANA_URL} search=p03-vehicle"
