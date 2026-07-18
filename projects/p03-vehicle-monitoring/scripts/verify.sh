#!/usr/bin/env bash
# p03 VEH-02/VEH-04 验收：ClickHouse flinklab.vehicle_alerts 为唯一放行条件。
# Kafka vehicle.alerts 仅作可选诊断日志，不得单独放行。
#
# 环境变量（可选）：
#   PATTERN_ID  白名单 pattern_id，默认 HARSH_THEN_FAULT（D-08/D-10）
#   MIN_COUNT   MATCH 行数下限，默认 1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi

PATTERN_ID="${PATTERN_ID:-HARSH_THEN_FAULT}"
MIN_COUNT="${MIN_COUNT:-1}"

# T-02-01：仅允许白名单三常量拼入查询；禁止未校验用户字符串进 SQL
case "${PATTERN_ID}" in
  HARSH_THEN_FAULT|TRIPLE_HARSH|DTC_PAIR)
    ;;
  *)
    echo "FAIL: PATTERN_ID 非法（仅允许 HARSH_THEN_FAULT|TRIPLE_HARSH|DTC_PAIR），got=${PATTERN_ID}" >&2
    exit 1
    ;;
esac

if ! [[ "${MIN_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "FAIL: MIN_COUNT 必须为非负整数，got=${MIN_COUNT}" >&2
  exit 1
fi

# 白名单分支后拼入固定 pattern_id 字面量（仍禁止任意外部输入直拼）
CH_MATCH_QUERY="SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH' AND pattern_id='${PATTERN_ID}'"
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

if [[ "${MATCH_COUNT}" -lt "${MIN_COUNT}" ]]; then
  echo "FAIL: expected vehicle_alerts MATCH pattern_id=${PATTERN_ID} count >= ${MIN_COUNT}, got match=${MATCH_COUNT} total=${TOTAL_COUNT}" >&2
  diag_kafka
  exit 1
fi

echo "ok alerts_match=${MATCH_COUNT} pattern_id=${PATTERN_ID} min_count=${MIN_COUNT} alerts_total=${TOTAL_COUNT}"
