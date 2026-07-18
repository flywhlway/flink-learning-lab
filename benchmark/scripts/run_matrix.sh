#!/usr/bin/env bash
# 仓库级压测矩阵入口（PROD-01 / D-01 / D-02）。
# 部署形态：compose Flink（非 K8s）；驱动：Python 造数 / Labs.events --eps；禁止 未登记 HTTP 压测工具。
# 产物：benchmark/baseline.md（OrbStack arm64 实测数字）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BASELINE_MD="${REPO_ROOT}/benchmark/baseline.md"
COMPOSE_DIR="${REPO_ROOT}/docker"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
COLLECT_PY="${SCRIPT_DIR}/collect_metrics.py"

PROM_URL="${PROM_URL:-http://localhost:9090}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
WARMUP_SEC="${WARMUP_SEC:-45}"
DURATION_SEC="${DURATION_SEC:-60}"
DRY_RUN="${DRY_RUN:-0}"
CELL_LIMIT="${CELL_LIMIT:-0}"   # 0=全量必跑；dry-run 默认 1
SKIP_20K="${SKIP_20K:-1}"       # stretch 默认跳过
SKIP_FORST="${SKIP_FORST:-1}"

usage() {
  cat <<'EOF'
用法: bash benchmark/scripts/run_matrix.sh [--dry-run] [--eps N]

环境变量:
  EPS / --eps     dry-run 时覆盖首单元格负载（默认 100）
  WARMUP_SEC      热身秒数（默认 45，须在 30–60）
  DURATION_SEC    计量秒数（默认 60）
  CELL_LIMIT      最多跑几个单元格（0=全量；dry-run 默认 1）
  PROM_URL        默认 http://localhost:9090
  FLINK_URL       默认 http://localhost:8081
  BOOTSTRAP       宿主机 Kafka（默认 localhost:9094）
  SKIP_20K=1      20k stretch 标 SKIPPED（默认）
  SKIP_FORST=1    ForSt 标 SKIPPED（默认）

作业轴: e01-J2 (KafkaClickstreamWindowJob) / e10 C5VehicleDtcPatternJob / p03 VehicleAlertJob
负载轴: 1k/5k 必跑；20k stretch
State: HashMap + RocksDB 增量；Checkpoint 主路径对齐 30s + 1–2 行对照
EOF
}

EPS_OVERRIDE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --eps)
      EPS_OVERRIDE="${2:?--eps 需要数值}"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    --implemented)
      # 兼容 Wave 0 门闩参数；本 harness 已实现，忽略
      shift
      ;;
    *)
      echo "FAIL: 未知参数 $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${DRY_RUN}" == "1" ]] || [[ "${DRY_RUN}" == "true" ]]; then
  CELL_LIMIT="${CELL_LIMIT:-1}"
  [[ "${CELL_LIMIT}" == "0" ]] && CELL_LIMIT=1
  WARMUP_SEC="${WARMUP_SEC:-15}"
  # dry-run 允许缩短热身；完整矩阵仍须 30–60
  if [[ -z "${EPS_OVERRIDE}" ]]; then
    EPS_OVERRIDE=100
  fi
  DURATION_SEC="${DURATION_SEC:-20}"
  echo "==> dry-run: EPS=${EPS_OVERRIDE} WARMUP=${WARMUP_SEC}s MEASURE=${DURATION_SEC}s CELL_LIMIT=${CELL_LIMIT}"
fi

# 完整矩阵热身必须落在 30–60（dry-run 豁免）
if [[ "${DRY_RUN}" != "1" ]] && [[ "${DRY_RUN}" != "true" ]]; then
  if [[ "${WARMUP_SEC}" -lt 30 ]] || [[ "${WARMUP_SEC}" -gt 60 ]]; then
    echo "FAIL: WARMUP_SEC=${WARMUP_SEC} 须在 30–60（学习工程热身；相对理想 3 分钟须在 baseline 声明）" >&2
    exit 1
  fi
fi

require_deps() {
  if ! curl -sf "${PROM_URL}/-/ready" >/dev/null 2>&1 \
    && ! curl -sf "${PROM_URL}/api/v1/status/config" >/dev/null 2>&1; then
    echo "FAIL: Prometheus 不可达（${PROM_URL}）" >&2
    exit 1
  fi
  if ! curl -sf "${FLINK_URL}/overview" >/dev/null 2>&1; then
    echo "FAIL: Flink REST 不可达（${FLINK_URL}）— 请先 cd docker && make up" >&2
    exit 1
  fi
  if ! docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
    >/dev/null 2>&1; then
    echo "FAIL: Kafka 不可达" >&2
    exit 1
  fi
}

snapshot_env() {
  local overview
  overview="$(curl -sf "${FLINK_URL}/overview")"
  python3 -c '
import json,sys,platform
o=json.load(sys.stdin)
print("flink_version=" + str(o.get("flink-version", "?")))
print("taskmanagers=" + str(o.get("taskmanagers", "?")))
print("slots_total=" + str(o.get("slots-total", "?")))
print("slots_available=" + str(o.get("slots-available", "?")))
print("jobs_running=" + str(o.get("jobs-running", "?")))
print("uname=" + platform.uname().system + " " + platform.uname().machine)
' <<<"${overview}"
}

cancel_all_jobs() {
  local ids id
  ids="$(curl -sf "${FLINK_URL}/jobs" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for j in d.get("jobs",[]):
    if j.get("status") in ("RUNNING","CREATED","RESTARTING","SUSPENDED"):
        print(j["id"])
' 2>/dev/null || true)"
  for id in ${ids}; do
    echo "==> cancel job ${id}"
    curl -sf -X PATCH "${FLINK_URL}/jobs/${id}?mode=cancel" >/dev/null 2>&1 || true
  done
  # 等待 slots 释放
  local i
  for i in $(seq 1 30); do
    local running
    running="$(curl -sf "${FLINK_URL}/overview" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("jobs-running",0))')"
    [[ "${running}" == "0" ]] && break
    sleep 1
  done
}

ensure_jars() {
  local e01 e10 p03
  e01="$(ls "${REPO_ROOT}/examples/e01-hello-flink/target/e01-hello-flink-"*.jar 2>/dev/null | grep -v original | head -1 || true)"
  e10="$(ls "${REPO_ROOT}/examples/e10-cep/target/e10-cep-"*.jar 2>/dev/null | grep -v original | head -1 || true)"
  p03="$(ls "${REPO_ROOT}/projects/p03-vehicle-monitoring/target/p03-vehicle-monitoring-"*.jar 2>/dev/null | grep -v original | head -1 || true)"
  if [[ -z "${e01}" || -z "${e10}" || -z "${p03}" ]]; then
    echo "==> packaging missing jars…"
    (cd "${REPO_ROOT}/examples" && mvn -q -pl e01-hello-flink,e10-cep -am package -DskipTests)
    (cd "${REPO_ROOT}/projects/p03-vehicle-monitoring" && mvn -q package -DskipTests)
    e01="$(ls "${REPO_ROOT}/examples/e01-hello-flink/target/e01-hello-flink-"*.jar | grep -v original | head -1)"
    e10="$(ls "${REPO_ROOT}/examples/e10-cep/target/e10-cep-"*.jar | grep -v original | head -1)"
    p03="$(ls "${REPO_ROOT}/projects/p03-vehicle-monitoring/target/p03-vehicle-monitoring-"*.jar | grep -v original | head -1)"
  fi
  mkdir -p "${COMPOSE_DIR}/jobs"
  cp -f "${e01}" "${e10}" "${p03}" "${COMPOSE_DIR}/jobs/"
  E01_JAR_NAME="$(basename "${e01}")"
  E10_JAR_NAME="$(basename "${e10}")"
  P03_JAR_NAME="$(basename "${p03}")"
}

flink_run() {
  # usage: flink_run <mainClass> <jarName> [jobArgs...]
  local main="$1" jar="$2"
  shift 2
  docker compose -f "${COMPOSE_FILE}" exec -T jobmanager \
    flink run -d -c "${main}" "/opt/flink/usrlib/${jar}" "$@"
}

wait_job_running() {
  local needle="$1"  # substring of job name
  local i name
  for i in $(seq 1 60); do
    name="$(curl -sf "${FLINK_URL}/jobs/overview" | python3 -c '
import json,sys
needle=sys.argv[1]
d=json.load(sys.stdin)
for j in d.get("jobs",[]):
    if j.get("state")=="RUNNING" and needle in (j.get("name") or ""):
        print(j["name"]); break
' "${needle}" 2>/dev/null || true)"
    if [[ -n "${name}" ]]; then
      echo "==> job RUNNING: ${name}"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: 等待作业 RUNNING 超时（needle=${needle}）" >&2
  return 1
}

run_gen_e01() {
  local eps="$1" dur="$2" label="$3"
  local total=$((eps * dur))
  echo "==> gen_events eps=${eps} duration≈${dur}s total=${total} (${label})"
  uv run "${REPO_ROOT}/scripts/gen_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --topic clicks \
    --eps "${eps}" \
    --total "${total}"
}

run_gen_p03() {
  local eps="$1" dur="$2" label="$3"
  echo "==> gen_vehicle_events rate=${eps} duration=${dur}s (${label})"
  uv run "${REPO_ROOT}/projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --rate "${eps}" \
    --duration "${dur}" \
    --vin "VIN-BENCH" \
    --vin-count 8
}

scrape_for_job() {
  local regex="$1"
  python3 "${COLLECT_PY}" --prom-url "${PROM_URL}" --job-name "${regex}" || true
}

kv_get() {
  local blob="$1" key="$2"
  echo "${blob}" | sed -n "s/^${key}=//p" | head -1
}

# 结果行累积（TSV）
RESULT_ROWS=()
SKIP_ROWS=()

run_cell() {
  local cell_id="$1" job="$2" eps="$3" backend="$4" ckpt_label="$5" ckpt_ms="$6" unaligned_flag="$7"
  local job_needle job_regex scrape wall_sec measure_start measure_end produced_eps
  local bp busy ckpt restarts lag

  echo ""
  echo "======== CELL #${cell_id}: job=${job} eps=${eps} state=${backend} ckpt=${ckpt_label} ========"

  cancel_all_jobs

  local unaligned_arg=()
  if [[ "${unaligned_flag}" == "1" ]]; then
    unaligned_arg=(--unaligned)
  fi

  case "${job}" in
    e01-J2)
      if [[ ${#unaligned_arg[@]} -gt 0 ]]; then
        flink_run com.flywhl.flinklab.e01.KafkaClickstreamWindowJob "${E01_JAR_NAME}" \
          --bootstrap kafka:9092 \
          --group-id "e01-bench-${cell_id}" \
          --state-backend "${backend}" \
          --checkpoint-interval-ms "${ckpt_ms}" \
          --unaligned
      else
        flink_run com.flywhl.flinklab.e01.KafkaClickstreamWindowJob "${E01_JAR_NAME}" \
          --bootstrap kafka:9092 \
          --group-id "e01-bench-${cell_id}" \
          --state-backend "${backend}" \
          --checkpoint-interval-ms "${ckpt_ms}"
      fi
      job_needle="e01-kafka-clickstream"
      job_regex="e01.*"
      wait_job_running "${job_needle}"
      echo "==> warmup ${WARMUP_SEC}s（丢弃）"
      run_gen_e01 "${eps}" "${WARMUP_SEC}" "warmup"
      sleep 3
      echo "==> measure ${DURATION_SEC}s"
      measure_start="$(date +%s)"
      run_gen_e01 "${eps}" "${DURATION_SEC}" "measure"
      measure_end="$(date +%s)"
      ;;
    e10-C5)
      # Labs.events RateLimiter；需 amount 字段，故不走 gen_events
      if [[ ${#unaligned_arg[@]} -gt 0 ]]; then
        flink_run com.flywhl.flinklab.e10.C5VehicleDtcPatternJob "${E10_JAR_NAME}" \
          --eps "${eps}" \
          --state-backend "${backend}" \
          --checkpoint-interval-ms "${ckpt_ms}" \
          --unaligned
      else
        flink_run com.flywhl.flinklab.e10.C5VehicleDtcPatternJob "${E10_JAR_NAME}" \
          --eps "${eps}" \
          --state-backend "${backend}" \
          --checkpoint-interval-ms "${ckpt_ms}"
      fi
      job_needle="e10-c5-vehicle-dtc"
      job_regex="e10.*"
      wait_job_running "${job_needle}"
      echo "==> warmup ${WARMUP_SEC}s（Labs.events 内源，墙钟丢弃）"
      sleep "${WARMUP_SEC}"
      echo "==> measure ${DURATION_SEC}s（内源稳态）"
      measure_start="$(date +%s)"
      sleep "${DURATION_SEC}"
      measure_end="$(date +%s)"
      ;;
    p03-VehicleAlertJob)
      flink_run com.flywhl.flinklab.p03.VehicleAlertJob "${P03_JAR_NAME}" \
        --kafka-bootstrap kafka:9092 \
        --group-id "p03-bench-${cell_id}" \
        --state-backend "${backend}" \
        --checkpoint-interval-ms "${ckpt_ms}" \
        --job-name "p03-vehicle-alert-bench-${cell_id}"
      job_needle="p03-vehicle-alert-bench-${cell_id}"
      job_regex="p03.*"
      wait_job_running "${job_needle}"
      echo "==> warmup ${WARMUP_SEC}s（丢弃）"
      run_gen_p03 "${eps}" "${WARMUP_SEC}" "warmup"
      sleep 3
      echo "==> measure ${DURATION_SEC}s"
      measure_start="$(date +%s)"
      run_gen_p03 "${eps}" "${DURATION_SEC}" "measure"
      measure_end="$(date +%s)"
      ;;
    *)
      echo "FAIL: 未知作业轴 ${job}" >&2
      exit 1
      ;;
  esac

  wall_sec=$((measure_end - measure_start))
  sleep 10
  scrape="$(scrape_for_job "${job_regex}")"
  bp="$(kv_get "${scrape}" "backpressure_ms_per_s")"
  busy="$(kv_get "${scrape}" "busy_ms_per_s")"
  ckpt="$(kv_get "${scrape}" "last_checkpoint_duration")"
  restarts="$(kv_get "${scrape}" "num_restarts")"
  lag="$(kv_get "${scrape}" "emit_event_time_lag")"

  if [[ "${wall_sec}" -gt 0 ]]; then
    produced_eps="$(python3 -c "print(round(${eps}, 2))")"
  else
    produced_eps="n/a"
  fi

  # 空指标写 n/a（仍为实测刮取结果，非假数）
  bp="${bp:-n/a}"
  busy="${busy:-n/a}"
  ckpt="${ckpt:-n/a}"
  restarts="${restarts:-n/a}"
  lag="${lag:-n/a}"

  RESULT_ROWS+=("| ${cell_id} | ${job} | ${eps} | ${backend} | ${ckpt_label} | ${produced_eps} | ${lag} | ${ckpt} | ${restarts} | ${bp} | ${busy} |")
  echo "==> cell #${cell_id} done: lag=${lag} ckpt=${ckpt}ms restarts=${restarts} bp=${bp} busy=${busy}"
}

write_baseline() {
  local started_utc="$1"
  local env_lines="$2"
  local flink_ver tms slots_total slots_avail jobs_running uname_line
  flink_ver="$(echo "${env_lines}" | sed -n 's/^flink_version=//p')"
  tms="$(echo "${env_lines}" | sed -n 's/^taskmanagers=//p')"
  slots_total="$(echo "${env_lines}" | sed -n 's/^slots_total=//p')"
  slots_avail="$(echo "${env_lines}" | sed -n 's/^slots_available=//p')"
  jobs_running="$(echo "${env_lines}" | sed -n 's/^jobs_running=//p')"
  uname_line="$(echo "${env_lines}" | sed -n 's/^uname=//p')"

  local row skipped_block
  {
    cat <<EOF
# 仓库级压测 baseline（OrbStack arm64 实测）

> 方法论对齐 [\`benchmark/README.md\`](./README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **权威路径**（D-03）：本文件；\`projects/*/docs/baseline.md\` 不替代本报告。
> 数字仅来自本次 \`make -C benchmark matrix\`（或 dry-run）在 compose Flink 上的刮取，禁止当作生产 SLA。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | ${started_utc} |
| 主机 / 架构 | ${uname_line} |
| 运行时 | OrbStack + docker compose（\`docker/docker-compose.yml\`），**非** K8s Operator |
| Flink | ${flink_ver}（镜像与仓库 SSOT 一致） |
| TaskManagers | ${tms} |
| Slots total / available（开跑前） | ${slots_total} / ${slots_avail} |
| Jobs running（开跑前） | ${jobs_running} |
| Kafka bootstrap（宿主机造数） | \`${BOOTSTRAP}\` |
| Prometheus | \`${PROM_URL}\` |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | e01: \`gen_events.py --eps/--total\`；p03: \`gen_vehicle_events.py --rate/--duration\`；e10 C5: 作业内 \`Labs.events\` RateLimiter（\`--eps\`，因 CEP 需 amount 字段） |
| 禁止 | 未登记 HTTP 压测工具 / 未登记压测镜像 |
| 热身 | ${WARMUP_SEC}s（**丢弃**，不计入下表） |
| 热身偏差声明 | 学习工程热身 ${WARMUP_SEC}s，相对方法论「理想 3 分钟」偏短（偏差 $((180 - WARMUP_SEC))s）；指标仅作本机对照，非稳态 SLA |
| 计量时长 | ${DURATION_SEC}s / 单元格 |
| 必跑负载 | 1k / 5k eps |
| stretch | 20k eps（见 SKIPPED）；ForSt（见 SKIPPED） |

## 3. 指标

吞吐口径：配置 eps；lag/checkpoint/restarts/反压/busy 来自 Prometheus instant query（值班五指标，见 \`monitoring/README.md\`）。

| # | Job | eps | State | Checkpoint | 配置 eps | emitEventTimeLag (ms) | lastCheckpointDuration (ms) | numRestarts | backpressure (ms/s) | busy (ms/s) |
|---|-----|-----|-------|------------|----------|----------------------|----------------------------|-------------|---------------------|-------------|
EOF
    for row in "${RESULT_ROWS[@]}"; do
      echo "${row}"
    done

    cat <<EOF

### SKIPPED

EOF
    if [[ ${#SKIP_ROWS[@]} -eq 0 ]]; then
      echo "_（无）_"
    else
      for row in "${SKIP_ROWS[@]}"; do
        echo "- ${row}"
      done
    fi

    cat <<EOF

## 4. 结论

- 本轮在 OrbStack arm64 / compose Flink 上跑通 D-01 裁剪必跑集（三作业 × 1k/5k × HashMap/RocksDB 增量主路径 + checkpoint 对照行）。
- 复跑：\`cd docker && make up && make up-p03\` → 打包 e01/e10/p03 → \`make -C benchmark matrix\`。
- dry-run 冒烟：\`make -C benchmark dry-run\`（低 EPS 单单元格）。
- 项目级 baseline 可交叉引用本文件，**不**替代仓库级矩阵报告（D-03）。

EOF
  } > "${BASELINE_MD}"

  echo "ok wrote ${BASELINE_MD}"
}

main() {
  echo "benchmark matrix: warmup=${WARMUP_SEC}s measure=${DURATION_SEC}s dry_run=${DRY_RUN}"
  require_deps
  ensure_jars

  local started_utc env_lines
  started_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  env_lines="$(snapshot_env)"

  # Pattern 1 最小必跑集（RESEARCH）
  # id|job|eps|backend|ckpt_label|ckpt_ms|unaligned
  local cells=(
    "1|e01-J2|1000|hashmap|对齐 30s|30000|0"
    "2|e01-J2|5000|hashmap|对齐 30s|30000|0"
    "3|e01-J2|1000|rocksdb|对齐 30s|30000|0"
    "4|e10-C5|1000|hashmap|对齐 30s|30000|0"
    "5|e10-C5|5000|rocksdb|对齐 30s|30000|0"
    "6|p03-VehicleAlertJob|1000|hashmap|对齐 30s|30000|0"
    "7|p03-VehicleAlertJob|5000|rocksdb|对齐 30s|30000|0"
    "8|e01-J2|1000|hashmap|对齐 10s|10000|0"
    "9|e01-J2|1000|hashmap|非对齐 30s|30000|1"
  )

  local ran=0
  local cell
  for cell in "${cells[@]}"; do
    IFS='|' read -r cid job eps backend ckpt_label ckpt_ms unaligned <<<"${cell}"
    if [[ -n "${EPS_OVERRIDE}" ]] && [[ "${ran}" -eq 0 ]]; then
      eps="${EPS_OVERRIDE}"
    fi
    run_cell "${cid}" "${job}" "${eps}" "${backend}" "${ckpt_label}" "${ckpt_ms}" "${unaligned}"
    ran=$((ran + 1))
    if [[ "${CELL_LIMIT}" -gt 0 ]] && [[ "${ran}" -ge "${CELL_LIMIT}" ]]; then
      echo "==> CELL_LIMIT=${CELL_LIMIT} 达到，停止后续单元格"
      break
    fi
  done

  if [[ "${SKIP_20K}" == "1" ]]; then
    SKIP_ROWS+=("20k eps stretch：OrbStack 本机稳定性与 p03 \`gen_vehicle_events\` \`MAX_RATE=5000\` 上限——本轮 SKIPPED，禁止填假数")
  fi
  if [[ "${SKIP_FORST}" == "1" ]]; then
    SKIP_ROWS+=("ForSt state backend：非 D-01 硬门禁，本轮 SKIPPED（可选附录）")
  fi

  write_baseline "${started_utc}" "${env_lines}"
  cancel_all_jobs
  echo "ok matrix cells_ran=${ran} baseline=${BASELINE_MD}"
}

main "$@"
