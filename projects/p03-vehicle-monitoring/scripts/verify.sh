#!/usr/bin/env bash
# p03 VEH-02 验收：ClickHouse flinklab.vehicle_alerts 为唯一放行条件。
# Kafka vehicle.alerts 仅作可选诊断日志，不得单独放行。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi

# 固定表名与查询，禁止拼接外部输入（T-1-01）
# 权威断言：MATCH 告警行数（亦可退化为总 count，此处优先 MATCH）
CH_MATCH_QUERY="SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH'"
CH_TOTAL_QUERY="SELECT count() FROM flinklab.vehicle_alerts"

diag_kafka() {
  # 可选诊断：失败时打印消费条数，但绝不因此放行
  local n
  n="$(docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic vehicle.alerts \
    --from-beginning \
    --timeout-ms 3000 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
  echo "diag: Kafka vehicle.alerts approx_lines=${n:-0}（仅排障，不作为放行条件）" >&2
}

if ! MATCH_COUNT="$(docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "${CH_MATCH_QUERY}" 2>/dev/null)"; then
  echo "FAIL: 无法查询 flinklab.vehicle_alerts（ClickHouse 未起或表不存在）" >&2
  diag_kafka
  exit 1
fi

MATCH_COUNT="$(printf '%s' "${MATCH_COUNT}" | tr -d '[:space:]')"
if [[ -z "${MATCH_COUNT}" ]]; then
  MATCH_COUNT=0
fi

# 兼容：若 MATCH 过滤无行但总表有行，仍以 MATCH 为准（总 count 仅诊断）
if ! TOTAL_COUNT="$(docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "${CH_TOTAL_QUERY}" 2>/dev/null)"; then
  TOTAL_COUNT="?"
fi
TOTAL_COUNT="$(printf '%s' "${TOTAL_COUNT}" | tr -d '[:space:]')"

if [[ "${MATCH_COUNT}" -lt 1 ]]; then
  echo "FAIL: expected vehicle_alerts MATCH count >= 1, got match=${MATCH_COUNT} total=${TOTAL_COUNT}" >&2
  diag_kafka
  exit 1
fi

echo "ok alerts_match=${MATCH_COUNT} alerts_total=${TOTAL_COUNT}"
