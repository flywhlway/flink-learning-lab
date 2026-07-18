#!/usr/bin/env bash
# p02 RECO-02 验收：ClickHouse flinklab.reco_results 为唯一放行条件（D-06/D-10）。
# Kafka reco.results / Redis feature:* 仅作可选诊断，不得单独放行。
#
# 环境变量（可选）：
#   FEATURE_SOURCE  白名单 feature_source，空=不按源过滤；允许 REDIS|STATE_ONLY（T-05-01）
#   MIN_COUNT       行数下限，默认 1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi

FEATURE_SOURCE="${FEATURE_SOURCE:-}"
MIN_COUNT="${MIN_COUNT:-1}"

# T-05-01：仅允许白名单枚举拼入查询；禁止未校验用户字符串进 SQL
if [[ -n "${FEATURE_SOURCE}" ]]; then
  case "${FEATURE_SOURCE}" in
    REDIS|STATE_ONLY)
      ;;
    *)
      echo "FAIL: FEATURE_SOURCE 非法（仅允许 REDIS|STATE_ONLY），got=${FEATURE_SOURCE}" >&2
      exit 1
      ;;
  esac
fi

if ! [[ "${MIN_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "FAIL: MIN_COUNT 必须为非负整数，got=${MIN_COUNT}" >&2
  exit 1
fi

if [[ -n "${FEATURE_SOURCE}" ]]; then
  CH_MATCH_QUERY="SELECT count() FROM flinklab.reco_results WHERE feature_source='${FEATURE_SOURCE}'"
else
  CH_MATCH_QUERY="SELECT count() FROM flinklab.reco_results"
fi
CH_TOTAL_QUERY="SELECT count() FROM flinklab.reco_results"

diag_kafka() {
  local n
  n="$(docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic reco.results \
    --from-beginning \
    --timeout-ms 3000 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
  echo "diag: Kafka reco.results approx_lines=${n:-0}（仅排障，不作为放行条件）" >&2
}

diag_redis() {
  # 禁止用 Redis 扫描结果单独放行（D-10）；此处只打印诊断
  local n
  n="$(docker compose -f "${COMPOSE_FILE}" exec -T redis \
    redis-cli --scan --pattern 'feature:*' 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
  echo "diag: Redis feature:* approx_keys=${n:-0}（仅排障，不作为放行条件）" >&2
}

if ! MATCH_COUNT="$(docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "${CH_MATCH_QUERY}" 2>/dev/null)"; then
  echo "FAIL: 无法查询 flinklab.reco_results（ClickHouse 未起或表不存在）" >&2
  diag_kafka
  diag_redis
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
  echo "FAIL: expected reco_results count >= ${MIN_COUNT}, got match=${MATCH_COUNT} total=${TOTAL_COUNT} feature_source=${FEATURE_SOURCE:-ANY}" >&2
  diag_kafka
  diag_redis
  exit 1
fi

echo "ok reco_results_match=${MATCH_COUNT} feature_source=${FEATURE_SOURCE:-ANY} min_count=${MIN_COUNT} reco_results_total=${TOTAL_COUNT}"
