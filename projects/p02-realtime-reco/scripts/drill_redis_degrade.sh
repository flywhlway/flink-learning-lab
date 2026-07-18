#!/usr/bin/env bash
# p02 RECO-03 / D-12 演练 #1：Redis 不可用时作业凭 Keyed State 仍产出 CH 行。
#
# 步骤：
#   1) 确保 p02 作业 RUNNING（必要时 cancel+submit）
#   2) TRUNCATE reco_results
#   3) compose stop redis（容器 fll-redis）
#   4) gen feature-score → 轮询 verify FEATURE_SOURCE=STATE_ONLY
#   5) 恢复 redis（trap EXIT 保证）
# 权威出口：ClickHouse feature_source=STATE_ONLY；禁止以 Redis KEYS / Kafka 单独放行。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
P02_DIR="${REPO_ROOT}/projects/p02-realtime-reco"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"
VERIFY_SH="${SCRIPT_DIR}/verify.sh"

FLINK_URL="${FLINK_URL:-http://localhost:8081}"
VERIFY_WAIT_SEC="${VERIFY_WAIT_SEC:-90}"
FEATURE_SOURCE="STATE_ONLY"
REDIS_STOPPED=0

TIMELINE=()

log_step() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  TIMELINE+=("${ts}  ${msg}")
  echo "==> [${ts}] ${msg}"
}

restore_redis() {
  if [[ "${REDIS_STOPPED}" -eq 1 ]]; then
    echo "==> drill: 恢复 redis（compose start redis / fll-redis）"
    docker compose -f "${COMPOSE_FILE}" start redis >/dev/null 2>&1 \
      || docker start fll-redis >/dev/null 2>&1 \
      || true
    # 等待 healthy / PING
    local deadline now
    deadline=$(( $(date +%s) + 45 ))
    while true; do
      if docker compose -f "${COMPOSE_FILE}" exec -T redis redis-cli ping 2>/dev/null \
        | grep -qi PONG; then
        echo "drill: redis 已恢复（PING=PONG）"
        REDIS_STOPPED=0
        return 0
      fi
      now="$(date +%s)"
      if [[ "${now}" -ge "${deadline}" ]]; then
        echo "WARN: redis 恢复等待超时；请手动: docker compose -f ${COMPOSE_FILE} start redis" >&2
        return 1
      fi
      sleep 2
    done
  fi
}

on_exit() {
  local code=$?
  restore_redis || true
  if [[ "${#TIMELINE[@]}" -gt 0 ]]; then
    echo ""
    echo "---- drill timeline ----"
    printf '%s\n' "${TIMELINE[@]}"
    echo "------------------------"
  fi
  exit "${code}"
}
trap on_exit EXIT

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
  if ! docker compose -f "${COMPOSE_FILE}" exec -T redis redis-cli ping 2>/dev/null \
    | grep -qi PONG; then
    echo "FAIL: 演练前 fll-redis 须可 PING（先 docker compose up -d redis）" >&2
    exit 1
  fi
}

p02_status() {
  # 打印含 p02-realtime-reco 的作业行；stdout 供调用方 grep
  docker compose -f "${COMPOSE_FILE}" exec -T jobmanager flink list -r 2>/dev/null \
    || true
}

p02_running() {
  p02_status | grep -q 'p02-realtime-reco' || return 1
}

p02_restarting() {
  # flink list -r 含 RUNNING/RESTARTING；粗检日志行
  docker compose -f "${COMPOSE_FILE}" exec -T jobmanager flink list -a 2>/dev/null \
    | grep 'p02-realtime-reco' | grep -qi RESTARTING || return 1
}

ensure_job_running() {
  if p02_running && ! p02_restarting; then
    log_step "p02-realtime-reco 已 RUNNING"
    return 0
  fi
  log_step "提交 p02（cancel+submit；若 Redis 演练中挂起则显式重提）"
  (cd "${P02_DIR}" && make cancel-p02) || true
  sleep 3
  (cd "${P02_DIR}" && make submit)
  local deadline now
  deadline=$(( $(date +%s) + 60 ))
  while true; do
    if p02_running && ! p02_restarting; then
      log_step "p02-realtime-reco 进入 RUNNING"
      return 0
    fi
    now="$(date +%s)"
    if [[ "${now}" -ge "${deadline}" ]]; then
      echo "FAIL: 提交后 p02-realtime-reco 未进入 RUNNING" >&2
      p02_status || true
      exit 1
    fi
    sleep 2
  done
}

stop_redis() {
  log_step "compose stop redis（容器 fll-redis）"
  docker compose -f "${COMPOSE_FILE}" stop redis
  REDIS_STOPPED=1
  # 确认不可达
  if docker compose -f "${COMPOSE_FILE}" exec -T redis redis-cli ping >/dev/null 2>&1; then
    echo "FAIL: redis 仍可 exec，stop 未生效" >&2
    exit 1
  fi
  log_step "fll-redis 已停止（后续点查应失败 → STATE_ONLY）"
}

assert_job_not_stuck_restarting() {
  sleep 3
  if p02_restarting; then
    echo "WARN: p02 出现 RESTARTING；尝试 cancel+submit 后继续（文档化重提）" >&2
    log_step "作业 RESTARTING → cancel+submit（Redis 仍停）"
    (cd "${P02_DIR}" && make cancel-p02) || true
    sleep 3
    (cd "${P02_DIR}" && make submit)
    local deadline now
    deadline=$(( $(date +%s) + 60 ))
    while true; do
      if p02_running && ! p02_restarting; then
        log_step "重提后 p02 RUNNING（Redis 仍停）"
        return 0
      fi
      now="$(date +%s)"
      if [[ "${now}" -ge "${deadline}" ]]; then
        echo "FAIL: Redis 不可用期间作业长期 RESTARTING，无法完成降级演练" >&2
        exit 1
      fi
      sleep 2
    done
  fi
  if ! p02_running; then
    echo "FAIL: Redis 停服后 p02 作业不在 RUNNING 列表" >&2
    p02_status || true
    exit 1
  fi
  log_step "Redis 停服期间作业仍 RUNNING（未长期 RESTARTING）"
}

phase_verify_state_only() {
  log_step "TRUNCATE reco_results"
  docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "TRUNCATE TABLE flinklab.reco_results"
  echo "truncate flinklab.reco_results ok"

  log_step "造数 scenario=feature-score（Keyed State 可累积）"
  (cd "${P02_DIR}" && uv run scripts/gen_reco_events.py --scenario feature-score)
  sleep 5

  log_step "轮询 verify FEATURE_SOURCE=${FEATURE_SOURCE}（最多 ${VERIFY_WAIT_SEC}s）"
  local deadline now
  deadline=$(( $(date +%s) + VERIFY_WAIT_SEC ))
  while true; do
    if FEATURE_SOURCE="${FEATURE_SOURCE}" bash "${VERIFY_SH}"; then
      log_step "ok verify.sh FEATURE_SOURCE=${FEATURE_SOURCE}（CH 权威）"
      return 0
    fi
    now="$(date +%s)"
    if [[ "${now}" -ge "${deadline}" ]]; then
      echo "FAIL: drill_redis_degrade 超时（${VERIFY_WAIT_SEC}s）未见到 feature_source=${FEATURE_SOURCE}" >&2
      FEATURE_SOURCE="${FEATURE_SOURCE}" bash "${VERIFY_SH}" || true
      # 诊断：任意 feature_source 分布
      docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
        clickhouse-client --user flinklab --password flinklab123 \
        --query "SELECT feature_source, count() FROM flinklab.reco_results GROUP BY feature_source" \
        2>/dev/null || true
      exit 1
    fi
    sleep 3
  done
}

main() {
  echo "p02 drill_redis_degrade: stop fll-redis → STATE_ONLY → CH verify → restore redis"
  require_deps
  ensure_job_running
  # 给作业稳定一点时间
  sleep 5
  stop_redis
  assert_job_not_stuck_restarting
  phase_verify_state_only
  log_step "演练主路径成功；EXIT trap 将恢复 redis"
  echo "ok drill_redis_degrade complete（feature_source=STATE_ONLY → CH）"
}

main "$@"
