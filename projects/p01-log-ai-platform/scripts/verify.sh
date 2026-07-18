#!/usr/bin/env bash
# p01 LOG-01/02 验收：ClickHouse flinklab.log_results 为唯一放行条件（D-06/D-10）。
# Kafka logs.events 仅作可选诊断日志，不得单独放行。
#
# 环境变量（可选）：
#   RULE_LABEL  白名单 rule_label，默认 AUTH_FAIL（T-04-01）
#   MIN_COUNT   行数下限，默认 1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi

RULE_LABEL="${RULE_LABEL:-AUTH_FAIL}"
MIN_COUNT="${MIN_COUNT:-1}"

# T-04-01：仅允许白名单枚举拼入查询；禁止未校验用户字符串进 SQL
case "${RULE_LABEL}" in
  AUTH_FAIL|ERROR_BURST|NONE)
    ;;
  *)
    echo "FAIL: RULE_LABEL 非法（仅允许 AUTH_FAIL|ERROR_BURST|NONE），got=${RULE_LABEL}" >&2
    exit 1
    ;;
esac

if ! [[ "${MIN_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "FAIL: MIN_COUNT 必须为非负整数，got=${MIN_COUNT}" >&2
  exit 1
fi

CH_MATCH_QUERY="SELECT count() FROM flinklab.log_results WHERE rule_label='${RULE_LABEL}'"
CH_TOTAL_QUERY="SELECT count() FROM flinklab.log_results"

diag_kafka() {
  local n
  n="$(docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic logs.events \
    --from-beginning \
    --timeout-ms 3000 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
  echo "diag: Kafka logs.events approx_lines=${n:-0}（仅排障，不作为放行条件）" >&2
}

if ! MATCH_COUNT="$(docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "${CH_MATCH_QUERY}" 2>/dev/null)"; then
  echo "FAIL: 无法查询 flinklab.log_results（ClickHouse 未起或表不存在）" >&2
  diag_kafka
  exit 1
fi

MATCH_COUNT="$(printf '%s' "${MATCH_COUNT}" | tr -d '[:space:]')"
if [[ -z "${MATCH_COUNT}" ]]; then
  MATCH_COUNT=0
fi

if ! TOTAL_COUNT="$(docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "${CH_TOTAL_QUERY}" 2>/dev/null)"; then
  TOTAL_COUNT="?"
fi
TOTAL_COUNT="$(printf '%s' "${TOTAL_COUNT}" | tr -d '[:space:]')"

if [[ "${MATCH_COUNT}" -lt "${MIN_COUNT}" ]]; then
  echo "FAIL: expected log_results rule_label=${RULE_LABEL} count >= ${MIN_COUNT}, got match=${MATCH_COUNT} total=${TOTAL_COUNT}" >&2
  diag_kafka
  exit 1
fi

echo "ok log_results_match=${MATCH_COUNT} rule_label=${RULE_LABEL} min_count=${MIN_COUNT} log_results_total=${TOTAL_COUNT}"
