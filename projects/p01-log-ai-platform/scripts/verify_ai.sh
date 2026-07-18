#!/usr/bin/env bash
# p01 LOG-03 可选 AI 验收（D-06）：
#   前置：宿主机 Ollama /api/tags 可达
#   权威出口：ClickHouse flinklab.log_results
#            ai_source='AI' AND ai_risk IN ('HIGH','MEDIUM','LOW')
#   主 CI/Phase 门禁仍以 verify.sh（规则路径）为准；本脚本失败不代表默认 verify 失败。
#
# 环境变量（可选）：
#   OLLAMA_URL   默认 http://127.0.0.1:11434
#   MIN_COUNT    行数下限，默认 1
#   AI_RISK      可选：仅匹配某一枚举（HIGH|MEDIUM|LOW）；默认三者皆可
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi

OLLAMA_URL="${OLLAMA_URL:-http://127.0.0.1:11434}"
MIN_COUNT="${MIN_COUNT:-1}"
AI_RISK="${AI_RISK:-}"

if ! [[ "${MIN_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "FAIL: MIN_COUNT 必须为非负整数，got=${MIN_COUNT}" >&2
  exit 1
fi

# T-04-01：ai_risk 白名单；禁止未校验字符串进 SQL
if [[ -n "${AI_RISK}" ]]; then
  case "${AI_RISK}" in
    HIGH|MEDIUM|LOW) ;;
    *)
      echo "FAIL: AI_RISK 非法（仅允许 HIGH|MEDIUM|LOW），got=${AI_RISK}" >&2
      exit 1
      ;;
  esac
fi

echo "==> 前置：探测 Ollama ${OLLAMA_URL}/api/tags（非默认门禁；失败说明需本机 ollama serve）"
if ! curl -sf --max-time 5 "${OLLAMA_URL}/api/tags" >/dev/null; then
  echo "FAIL: Ollama 不可达（${OLLAMA_URL}/api/tags）" >&2
  echo "hint: 本脚本是可选 AI 验收轨，不替代 make verify；请先 ollama serve 并确认 ollama list 有模型" >&2
  echo "hint: 作业可用 --ai.model 覆盖（本机常见 qwen3.5:9b-mlx，默认配置为 qwen3:8b）" >&2
  exit 1
fi
echo "ok ollama_tags reachable url=${OLLAMA_URL}"

if [[ -n "${AI_RISK}" ]]; then
  CH_MATCH_QUERY="SELECT count() FROM flinklab.log_results WHERE ai_source='AI' AND ai_risk='${AI_RISK}'"
else
  CH_MATCH_QUERY="SELECT count() FROM flinklab.log_results WHERE ai_source='AI' AND ai_risk IN ('HIGH','MEDIUM','LOW')"
fi
CH_TOTAL_QUERY="SELECT count() FROM flinklab.log_results"
CH_SAMPLE_QUERY="SELECT service, level, ai_risk, ai_source, rule_label FROM flinklab.log_results WHERE ai_source='AI' LIMIT 5"

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
  echo "FAIL: expected log_results ai_source=AI ai_risk∈{HIGH,MEDIUM,LOW} count >= ${MIN_COUNT}, got match=${MATCH_COUNT} total=${TOTAL_COUNT}" >&2
  echo "diag: sample AI rows:" >&2
  docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "${CH_SAMPLE_QUERY}" 2>/dev/null || true
  echo "diag: 若全是 DEGRADED/DISABLED：检查作业 --ai.enabled=true、--ai.model 是否在 ollama list、timeout 是否过短" >&2
  diag_kafka
  exit 1
fi

echo "ok ai_source=AI ai_risk_match=${MATCH_COUNT} min_count=${MIN_COUNT} log_results_total=${TOTAL_COUNT}"
