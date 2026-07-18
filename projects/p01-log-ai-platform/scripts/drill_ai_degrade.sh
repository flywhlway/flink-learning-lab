#!/usr/bin/env bash
# p01 LOG-02 / D-14 演练：AI off 或 endpoint 不可达时，规则路径 verify 仍绿。
#
# 纪律：
#   1) 确保作业以 --ai.enabled=false 提交（或取消后重提）
#   2) 将 ai.endpoint 指向不可达地址（作业仍不应依赖 AI）
#   3) truncate → gen rule-auth-fail → 轮询 verify.sh → 期望 exit 0
# 失败非 0。额外 chaos（杀 TM）见 README 可选附录，不挡本脚本 exit 0。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
P01_DIR="${REPO_ROOT}/projects/p01-log-ai-platform"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"
VERIFY_SH="${SCRIPT_DIR}/verify.sh"

FLINK_URL="${FLINK_URL:-http://localhost:8081}"
VERIFY_WAIT_SEC="${VERIFY_WAIT_SEC:-90}"
RULE_LABEL="${RULE_LABEL:-AUTH_FAIL}"
# 故意不可达：证明默认门禁不依赖 Ollama / 外部模型
AI_ENDPOINT_UNREACHABLE="${AI_ENDPOINT_UNREACHABLE:-http://127.0.0.1:9}"

require_deps() {
  if ! curl -sf "${FLINK_URL}/overview" >/dev/null 2>&1; then
    echo "FAIL: Flink REST 不可达（${FLINK_URL}）" >&2
    exit 1
  fi
  if ! docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "SELECT 1" >/dev/null 2>&1; then
    echo "FAIL: ClickHouse 不可达" >&2
    exit 1
  fi
}

p01_running() {
  docker compose -f "${COMPOSE_FILE}" exec -T jobmanager flink list -r 2>/dev/null \
    | grep -q 'p01-log-ai' || return 1
}

submit_ai_off() {
  echo "==> drill: 提交 p01（--ai.enabled=false + 不可达 ai.endpoint）"
  local jar jar_name
  jar="$(ls "${P01_DIR}"/target/p01-log-ai-platform-*.jar 2>/dev/null | grep -v original | head -1 || true)"
  if [[ -z "${jar}" ]]; then
    echo "==> drill: 无 jar，先 package"
    (cd "${P01_DIR}" && make package)
    jar="$(ls "${P01_DIR}"/target/p01-log-ai-platform-*.jar 2>/dev/null | grep -v original | head -1)"
  fi
  if [[ -z "${jar}" ]]; then
    echo "FAIL: 未找到 p01-log-ai-platform-*.jar" >&2
    exit 1
  fi
  mkdir -p "${REPO_ROOT}/docker/jobs"
  cp "${jar}" "${REPO_ROOT}/docker/jobs/"
  jar_name="$(basename "${jar}")"

  (cd "${P01_DIR}" && make cancel-p01) || true
  sleep 3

  cd "${REPO_ROOT}/docker" && docker compose exec -T jobmanager flink run -d \
    -c com.flywhl.flinklab.p01.LogAiJob \
    "/opt/flink/usrlib/${jar_name}" \
    --ai.enabled=false \
    --ai.endpoint="${AI_ENDPOINT_UNREACHABLE}" \
    --group-id=p01-log-ai-drill-degrade

  local deadline now
  deadline=$(( $(date +%s) + 60 ))
  while true; do
    if p01_running; then
      echo "drill: p01-log-ai RUNNING（ai.enabled=false endpoint=${AI_ENDPOINT_UNREACHABLE}）"
      return 0
    fi
    now="$(date +%s)"
    if [[ "${now}" -ge "${deadline}" ]]; then
      echo "FAIL: 提交后 p01-log-ai 未进入 RUNNING" >&2
      exit 1
    fi
    sleep 2
  done
}

assert_ai_unreachable_or_off() {
  # 文档化前提：本演练不要求 Ollama；若本机 Ollama 在跑也不使用
  if curl -sf --max-time 2 http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
    echo "drill: 本机 Ollama 可达，但作业已 --ai.enabled=false + endpoint=${AI_ENDPOINT_UNREACHABLE}（不调用）"
  else
    echo "drill: 本机 Ollama 不可达（符合 AI off / 不可用前提）"
  fi
}

phase_verify_rule_path() {
  echo "==> drill: truncate → gen rule-auth-fail → 轮询 verify（RULE_LABEL=${RULE_LABEL}）"
  docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "TRUNCATE TABLE flinklab.log_results"
  echo "truncate flinklab.log_results ok"

  uv run "${SCRIPT_DIR}/gen_log_events.py" --scenario rule-auth-fail
  sleep 3

  local deadline now
  deadline=$(( $(date +%s) + VERIFY_WAIT_SEC ))
  while true; do
    if RULE_LABEL="${RULE_LABEL}" bash "${VERIFY_SH}"; then
      echo "drill: ok verify.sh RULE_LABEL=${RULE_LABEL}（AI off / endpoint 不可达仍绿）"
      return 0
    fi
    now="$(date +%s)"
    if [[ "${now}" -ge "${deadline}" ]]; then
      echo "FAIL: drill_ai_degrade 超时（${VERIFY_WAIT_SEC}s）verify.sh 未绿" >&2
      exit 1
    fi
    sleep 3
  done
}

main() {
  echo "p01 drill_ai_degrade: AI off + unreachable endpoint → verify 仍绿"
  require_deps
  assert_ai_unreachable_or_off
  submit_ai_off
  # 给作业接 Kafka / 稳定一点时间
  sleep 8
  phase_verify_rule_path
  echo "ok drill_ai_degrade complete（ai.enabled=false → CH AUTH_FAIL）"
}

main "$@"
