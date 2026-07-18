#!/usr/bin/env bash
# p03 VEH-06 watermark 停滞演练（D-09/D-10/D-11）。
#
# 两阶段 stall → recover（禁止仅靠空闲分区；禁止以 Flink REST /watermarks 为唯一断言）：
#   1) stall: --partial harsh-open + 冻结 eventTime=T0 的 HEARTBEAT 涓流 ≥45s
#      断言：CH MATCH 不增；CEP currentInputWatermark 停滞 ⇒ 推算 event-time lag 上升
#      （Source currentEmitEventTimeLag 在 assignTimestamps 下游链路上常近 0，不作唯一断言）
#   2) recover: match-harsh-fault（advancing 尾心跳）+ PATTERN_ID verify.sh
# 失败非 0。可选附录（杀 TM）见 README，不挡 exit 0。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

PROM_URL="${PROM_URL:-http://localhost:9090}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
STALL_SEC="${STALL_SEC:-50}"
FROZEN_RATE="${FROZEN_RATE:-4}"
FROZEN_VIN_COUNT="${FROZEN_VIN_COUNT:-8}"
VIN="${VIN:-VIN-P03-STALL-$(date +%s)}"
PATTERN_ID="${PATTERN_ID:-HARSH_THEN_FAULT}"
RECOVER_WAIT_SEC="${RECOVER_WAIT_SEC:-60}"
# 冻结 ≥45s 后，墙钟相对 CEP watermark 的推算 lag 至少增此毫秒数
LAG_GROWTH_MS="${LAG_GROWTH_MS:-20000}"
OOO_MS="${OOO_MS:-5000}"

if [[ "${STALL_SEC}" -lt 45 ]]; then
  echo "FAIL: STALL_SEC 须 ≥45（冻结心跳，非 idle 分区演示），got=${STALL_SEC}" >&2
  exit 1
fi

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

# CEP 侧 watermark（比 Source currentEmitEventTimeLag 更能反映 assignTimestamps 后水位）
prom_cep_watermark_ms() {
  local json
  json="$(curl -sfG "${PROM_URL}/api/v1/query" --data-urlencode \
    'query=flink_taskmanager_job_task_currentInputWatermark{job_name="p03_vehicle_alert",task_name="cep_harsh_then_fault"}' \
    2>/dev/null)" || return 1
  python3 -c '
import json,sys
d=json.load(sys.stdin)
vals=[]
for r in d.get("data",{}).get("result",[]):
    try:
        v=float(r["value"][1])
    except (KeyError, IndexError, TypeError, ValueError):
        continue
    if v > -1e18:
        vals.append(v)
print(max(vals) if vals else "")
' <<<"${json}"
}

# 诊断用：Source emit lag（可近 0，不作为唯一放行条件）
prom_source_emit_lag_ms() {
  local json
  json="$(curl -sfG "${PROM_URL}/api/v1/query" --data-urlencode \
    'query=flink_taskmanager_job_task_operator_currentEmitEventTimeLag{job_name="p03_vehicle_alert",operator_name="Source:_kafka_vehicle_events"}' \
    2>/dev/null)" || return 1
  python3 -c '
import json,sys
d=json.load(sys.stdin)
vals=[]
for r in d.get("data",{}).get("result",[]):
    try:
        vals.append(float(r["value"][1]))
    except (KeyError, IndexError, TypeError, ValueError):
        pass
print(max(vals) if vals else "")
' <<<"${json}"
}

ch_match_count() {
  local pid="$1"
  docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH' AND pattern_id='${pid}'" \
    2>/dev/null | tr -d '[:space:]'
}

require_deps() {
  if ! curl -sf "${PROM_URL}/-/ready" >/dev/null 2>&1 \
    && ! curl -sf "${PROM_URL}/api/v1/status/config" >/dev/null 2>&1; then
    echo "FAIL: Prometheus 不可达（${PROM_URL}）" >&2
    exit 1
  fi
  if ! curl -sf "${FLINK_URL}/overview" >/dev/null 2>&1; then
    echo "FAIL: Flink REST 不可达（${FLINK_URL}）" >&2
    exit 1
  fi
  local running
  running="$(curl -sf "${FLINK_URL}/overview" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("jobs-running",0))')"
  if [[ "${running}" -lt 1 ]]; then
    echo "FAIL: 无 RUNNING Flink 作业；请先 make submit（及可选 submit-window）" >&2
    exit 1
  fi
  if ! docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "SELECT 1" >/dev/null 2>&1; then
    echo "FAIL: ClickHouse 不可达" >&2
    exit 1
  fi
}

wait_cep_watermark_near() {
  # 等待 CEP watermark 贴近 freeze_at - ooo（Prometheus 刮取有延迟）
  local freeze_at="$1"
  local expect=$((freeze_at - OOO_MS))
  local deadline=$(( $(date +%s) + 45 ))
  local wm
  echo "stall: 等待 CEP watermark ≈ ${expect}（freeze_at=${freeze_at} ooo=${OOO_MS}）"
  while true; do
    wm="$(prom_cep_watermark_ms || true)"
    if [[ -n "${wm}" ]]; then
      if python3 -c "import sys; sys.exit(0 if abs(float('${wm}') - float('${expect}')) <= 3000 else 1)"; then
        echo "stall: CEP watermark ready wm=${wm}"
        return 0
      fi
      echo "stall: … wm=${wm} (expect≈${expect})"
    fi
    if [[ "$(date +%s)" -ge "${deadline}" ]]; then
      echo "FAIL: 等待 CEP watermark 超时（最后 wm=${wm:-empty} expect≈${expect}）" >&2
      echo "hint: 确认 p03_vehicle_alert RUNNING 且无大 backlog；必要时 reset-offsets --to-latest 后重提作业" >&2
      exit 1
    fi
    sleep 2
  done
}

phase_stall() {
  echo "==> drill phase=stall vin=${VIN} frozen HEARTBEAT ${STALL_SEC}s @ rate=${FROZEN_RATE} vin_count=${FROZEN_VIN_COUNT}"

  local ctrl_ver
  ctrl_ver="$(date +%s)"
  uv run "${SCRIPT_DIR}/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --publish-control "{\"activePatterns\":[\"HARSH_THEN_FAULT\"],\"version\":${ctrl_ver}}"

  docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "TRUNCATE TABLE flinklab.vehicle_alerts"
  echo "truncated flinklab.vehicle_alerts ok"

  local match_before
  match_before="$(ch_match_count "${PATTERN_ID}")"
  match_before="${match_before:-0}"
  echo "stall: MATCH count before=${match_before}"

  local partial_out freeze_at
  partial_out="$(uv run "${SCRIPT_DIR}/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --partial harsh-open \
    --vin "${VIN}" 2>&1)" || {
    echo "${partial_out}" >&2
    echo "FAIL: partial harsh-open 失败" >&2
    exit 1
  }
  echo "${partial_out}"
  freeze_at="$(printf '%s\n' "${partial_out}" | sed -n 's/.*freeze_hint=//p' | head -1 | tr -d '[:space:]')"
  if [[ -z "${freeze_at}" ]]; then
    echo "FAIL: 未能解析 freeze_hint（partial 输出）" >&2
    exit 1
  fi
  echo "stall: freeze_at=${freeze_at}"

  # 先短促冻结几秒，帮助 watermark 推到 T0-ooo，再采基线
  uv run "${SCRIPT_DIR}/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --frozen-event-time \
    --freeze-at "${freeze_at}" \
    --duration 5 \
    --rate "${FROZEN_RATE}" \
    --vin-count "${FROZEN_VIN_COUNT}" \
    --vin "${VIN}"

  wait_cep_watermark_near "${freeze_at}"

  local wm_before wall_before lag_before emit_lag_before
  wm_before="$(prom_cep_watermark_ms || true)"
  wall_before="$(now_ms)"
  if [[ -z "${wm_before}" ]]; then
    echo "FAIL: 无法读取 CEP currentInputWatermark（stall 前）" >&2
    exit 1
  fi
  lag_before="$(python3 -c "print(float('${wall_before}') - float('${wm_before}'))")"
  emit_lag_before="$(prom_source_emit_lag_ms || true)"
  echo "stall: wm_before=${wm_before} wall=${wall_before} computed_lag_ms=${lag_before} source_emit_lag_ms=${emit_lag_before:-n/a}"

  # 主冻结窗口 ≥45s：多 key HEARTBEAT，eventTime 固定 T0
  uv run "${SCRIPT_DIR}/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --frozen-event-time \
    --freeze-at "${freeze_at}" \
    --duration "${STALL_SEC}" \
    --rate "${FROZEN_RATE}" \
    --vin-count "${FROZEN_VIN_COUNT}" \
    --vin "${VIN}"

  sleep 3
  local wm_after wall_after lag_after match_during emit_lag_after wm_delta lag_growth
  wm_after="$(prom_cep_watermark_ms || true)"
  wall_after="$(now_ms)"
  if [[ -z "${wm_after}" ]]; then
    echo "FAIL: 无法读取 CEP currentInputWatermark（stall 后）" >&2
    exit 1
  fi
  lag_after="$(python3 -c "print(float('${wall_after}') - float('${wm_after}'))")"
  wm_delta="$(python3 -c "print(float('${wm_after}') - float('${wm_before}'))")"
  lag_growth="$(python3 -c "print(float('${lag_after}') - float('${lag_before}'))")"
  match_during="$(ch_match_count "${PATTERN_ID}")"
  match_during="${match_during:-0}"
  emit_lag_after="$(prom_source_emit_lag_ms || true)"

  echo "stall: wm_after=${wm_after} wall=${wall_after} computed_lag_ms=${lag_after}"
  echo "stall: wm_delta_ms=${wm_delta} lag_growth_ms=${lag_growth} MATCH during=${match_during}"
  echo "stall: source_emit_lag ${emit_lag_before:-n/a} → ${emit_lag_after:-n/a}（诊断；非唯一断言）"

  if [[ "${match_during}" -gt "${match_before}" ]]; then
    echo "FAIL: stall 期间 MATCH 不应增加（before=${match_before} during=${match_during}）" >&2
    exit 1
  fi

  # watermark 不得随墙钟明显前进（允许刮取噪声 ≤2s）
  if ! python3 -c "import sys; sys.exit(0 if float('${wm_delta}') <= 2000 else 1)"; then
    echo "FAIL: 冻结心跳期间 CEP watermark 仍在前进（wm_delta=${wm_delta}）— 可能有 advancing backlog" >&2
    echo "hint: reset consumer offsets to latest 后重提作业，勿在 loadtest  backlog 未消化时演练" >&2
    exit 1
  fi

  if ! python3 -c "import sys; sys.exit(0 if float('${lag_growth}') >= float('${LAG_GROWTH_MS}') else 1)"; then
    echo "FAIL: 推算 event-time lag 未上升（growth=${lag_growth} < ${LAG_GROWTH_MS}）" >&2
    exit 1
  fi

  echo "stall: ok（MATCH 未增；CEP WM 停滞；computed lag 上升；Flink UI Watermarks 列可作 human-check 副证）"
}

phase_recover() {
  echo "==> drill phase=recover scenario=match-harsh-fault vin=${VIN}"

  uv run "${SCRIPT_DIR}/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --scenario match-harsh-fault \
    --vin "${VIN}"

  local deadline now
  deadline=$(( $(date +%s) + RECOVER_WAIT_SEC ))
  while true; do
    if PATTERN_ID="${PATTERN_ID}" bash "${SCRIPT_DIR}/verify.sh"; then
      echo "recover: ok verify.sh PATTERN_ID=${PATTERN_ID}"
      return 0
    fi
    now="$(date +%s)"
    if [[ "${now}" -ge "${deadline}" ]]; then
      echo "FAIL: recover 超时（${RECOVER_WAIT_SEC}s）PATTERN_ID=${PATTERN_ID} verify.sh 未绿" >&2
      exit 1
    fi
    sleep 3
  done
}

main() {
  echo "p03 drill_watermark_stall: stall=${STALL_SEC}s vin=${VIN}"
  require_deps
  phase_stall
  phase_recover
  echo "ok drill_watermark_stall complete（stall→recover）"
}

main "$@"
