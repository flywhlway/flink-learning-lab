#!/usr/bin/env bash
# p03 VEH-05 大盘门禁（D-15）：dashboard JSON 非空 + Grafana HTTP 健康 + CH DS 插件。
# ClickHouse 仍是 CEP 告警权威出口（verify.sh）；本脚本断言大盘可导入/可发现。
#
# 环境变量（可选）：
#   GRAFANA_URL   默认 http://localhost:3000（禁止任意远程默认）
#   GRAFANA_USER  默认 admin
#   GRAFANA_PASS  默认 flinklab（与 compose GF_SECURITY_ADMIN_PASSWORD 对齐）
#   CH_URL        默认 http://localhost:8123
#   SKIP_CH_SMOKE 设为 1 时跳过 vehicle_window_metrics count smoke
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DASHBOARD_JSON="${REPO_ROOT}/projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json"

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASS="${GRAFANA_PASS:-flinklab}"
CH_URL="${CH_URL:-http://localhost:8123}"
SKIP_CH_SMOKE="${SKIP_CH_SMOKE:-0}"

if [[ ! -s "${DASHBOARD_JSON}" ]]; then
  echo "FAIL: 缺少非空 dashboard JSON: ${DASHBOARD_JSON}" >&2
  exit 1
fi

if ! grep -q 'p03-clickhouse' "${DASHBOARD_JSON}"; then
  echo "FAIL: dashboard JSON 未引用 uid p03-clickhouse" >&2
  exit 1
fi

if ! grep -qE 'Prometheus|PBFA97CFB590B2093' "${DASHBOARD_JSON}"; then
  echo "FAIL: dashboard JSON 未引用 Prometheus 数据源" >&2
  exit 1
fi

DS_JSON="$(curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  "${GRAFANA_URL}/api/datasources")" || {
  echo "FAIL: Grafana /api/datasources 不可达或认证失败（${GRAFANA_URL}）" >&2
  exit 1
}

if ! printf '%s' "${DS_JSON}" | grep -q 'grafana-clickhouse-datasource'; then
  echo "FAIL: Grafana 未安装或未启用 grafana-clickhouse-datasource 插件" >&2
  echo "  修复：确认 compose grafana 环境含 GF_INSTALL_PLUGINS=grafana-clickhouse-datasource 后重启：" >&2
  echo "    cd docker && docker compose up -d --force-recreate grafana" >&2
  echo "  离线 fallback（宿主机进容器）：" >&2
  echo "    docker compose exec grafana grafana-cli plugins install grafana-clickhouse-datasource" >&2
  echo "    docker compose restart grafana" >&2
  exit 1
fi

if ! printf '%s' "${DS_JSON}" | grep -q 'p03-clickhouse'; then
  echo "FAIL: Grafana /api/datasources 未发现 uid=p03-clickhouse（检查 clickhouse.yml provisioning）" >&2
  exit 1
fi

SEARCH_JSON="$(curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  "${GRAFANA_URL}/api/search?query=p03")" || {
  echo "FAIL: Grafana /api/search?query=p03 失败（${GRAFANA_URL}）" >&2
  exit 1
}

if ! printf '%s' "${SEARCH_JSON}" | grep -q 'p03-vehicle'; then
  echo "FAIL: Grafana search 未发现 p03-vehicle 大盘（query=p03）" >&2
  echo "  确认 dashboards 目录已挂载且含 p03-vehicle-overview.json，等待 provider 扫描（约 30s）" >&2
  exit 1
fi

if [[ "${SKIP_CH_SMOKE}" != "1" ]]; then
  METRICS_COUNT="$(curl -sf \
    "${CH_URL}/?user=flinklab&password=flinklab123" \
    --data-binary "SELECT count() FROM flinklab.vehicle_window_metrics" 2>/dev/null || echo "")"
  if [[ -z "${METRICS_COUNT}" ]]; then
    echo "FAIL: ClickHouse smoke 查询失败（${CH_URL}）；确认 make up-p03 已建表" >&2
    exit 1
  fi
  if [[ "${METRICS_COUNT}" -lt 1 ]]; then
    echo "FAIL: flinklab.vehicle_window_metrics count=${METRICS_COUNT}（须 ≥1；先 make submit-window && make gen）" >&2
    exit 1
  fi
  echo "ok ch_metrics_count=${METRICS_COUNT}"
fi

echo "ok dashboard_json=${DASHBOARD_JSON} grafana=${GRAFANA_URL} search=p03-vehicle ds=p03-clickhouse"
