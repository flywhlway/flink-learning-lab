#!/usr/bin/env bash
# p03 VEH-06 压测入口（D-06/D-07/D-08/D-11）：
#   gen --rate/--duration → 刮 Prometheus / 可选 CH → 写 docs/baseline.md
# 禁止 k6/JMeter；默认 bootstrap localhost:9094；依赖不可达 exit 1。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
P03_DIR="${REPO_ROOT}/projects/p03-vehicle-monitoring"
BASELINE_MD="${P03_DIR}/docs/baseline.md"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

PROM_URL="${PROM_URL:-http://localhost:9090}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
RATE="${RATE:-100}"
WARMUP_SEC="${WARMUP_SEC:-45}"
DURATION_SEC="${DURATION_SEC:-120}"
VIN="${VIN:-VIN-P03-LOAD}"
VIN_COUNT="${VIN_COUNT:-8}"

prom_query() {
  local q="$1"
  curl -sfG "${PROM_URL}/api/v1/query" --data-urlencode "query=${q}" 2>/dev/null \
    || return 1
}

prom_scalar_max() {
  # 取向量结果中 value 的最大值；无结果返回 empty
  local q="$1"
  local json
  json="$(prom_query "${q}")" || return 1
  python3 -c '
import json,sys
d=json.load(sys.stdin)
res=d.get("data",{}).get("result",[])
vals=[]
for r in res:
    try:
        vals.append(float(r["value"][1]))
    except (KeyError, IndexError, TypeError, ValueError):
        pass
print(max(vals) if vals else "")
' <<<"${json}"
}

prom_scalar_avg() {
  local q="$1"
  local json
  json="$(prom_query "${q}")" || return 1
  python3 -c '
import json,sys
d=json.load(sys.stdin)
res=d.get("data",{}).get("result",[])
vals=[]
for r in res:
    try:
        vals.append(float(r["value"][1]))
    except (KeyError, IndexError, TypeError, ValueError):
        pass
if not vals:
    print("")
else:
    print(sum(vals)/len(vals))
' <<<"${json}"
}

ch_count() {
  local sql="$1"
  docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "${sql}" 2>/dev/null | tr -d '[:space:]' || echo ""
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
  if ! docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
    clickhouse-client --user flinklab --password flinklab123 \
    --query "SELECT 1" >/dev/null 2>&1; then
    echo "FAIL: ClickHouse 不可达" >&2
    exit 1
  fi
  # Kafka：尝试 metadata（失败则 exit 1）
  if ! python3 -c "
from confluent_kafka.admin import AdminClient
c=AdminClient({'bootstrap.servers':'${BOOTSTRAP}'})
md=c.list_topics(timeout=5)
assert md is not None
" 2>/dev/null; then
    # 无 confluent_kafka 时用 docker kafka 探测
    if ! docker compose -f "${COMPOSE_FILE}" exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
      >/dev/null 2>&1; then
      echo "FAIL: Kafka 不可达（bootstrap=${BOOTSTRAP}）" >&2
      exit 1
    fi
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

run_gen() {
  local dur="$1"
  local label="$2"
  echo "==> gen rate=${RATE} duration=${dur}s (${label}) bootstrap=${BOOTSTRAP}"
  uv run "${SCRIPT_DIR}/gen_vehicle_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --rate "${RATE}" \
    --duration "${dur}" \
    --vin "${VIN}" \
    --vin-count "${VIN_COUNT}"
}

scrape_metrics() {
  # 输出 key=value 行
  local lag_alert ckpt_alert restarts_alert lag_window ckpt_window
  lag_alert="$(prom_scalar_max \
    'flink_taskmanager_job_task_operator_currentEmitEventTimeLag{job_name=~"p03.*",operator_name="Source:_kafka_vehicle_events"}' \
    || true)"
  ckpt_alert="$(prom_scalar_max \
    'flink_jobmanager_job_lastCheckpointDuration{job_name=~"p03.*"}' \
    || true)"
  restarts_alert="$(prom_scalar_max \
    'flink_jobmanager_job_numRestarts{job_name=~"p03.*"}' \
    || true)"
  lag_window="$(prom_scalar_avg \
    'flink_taskmanager_job_task_operator_currentEmitEventTimeLag{job_name="p03_vehicle_window_metrics",operator_name="Source:_kafka_vehicle_events"}' \
    || true)"
  ckpt_window="$(prom_scalar_max \
    'flink_jobmanager_job_lastCheckpointDuration{job_name="p03_vehicle_window_metrics"}' \
    || true)"

  echo "lag_vehicle_events_max_ms=${lag_alert:-n/a}"
  echo "ckpt_duration_max_ms=${ckpt_alert:-n/a}"
  echo "num_restarts_max=${restarts_alert:-n/a}"
  echo "lag_window_source_avg_ms=${lag_window:-n/a}"
  echo "ckpt_window_ms=${ckpt_window:-n/a}"
}

main() {
  echo "p03 loadtest: rate=${RATE} warmup=${WARMUP_SEC}s measure=${DURATION_SEC}s"
  require_deps

  local started_utc env_lines
  started_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  env_lines="$(snapshot_env)"

  local alerts_before metrics_before
  alerts_before="$(ch_count "SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH'")"
  metrics_before="$(ch_count "SELECT count() FROM flinklab.vehicle_window_metrics")"

  echo "==> warmup ${WARMUP_SEC}s（指标丢弃，不计入 baseline）"
  run_gen "${WARMUP_SEC}" "warmup"
  sleep 5

  echo "==> measure ${DURATION_SEC}s"
  local measure_start measure_end
  measure_start="$(date +%s)"
  run_gen "${DURATION_SEC}" "measure"
  measure_end="$(date +%s)"
  local wall_sec=$((measure_end - measure_start))

  # 给 sink / 窗口一点落地时间
  sleep 15

  local scrape alerts_after metrics_after
  scrape="$(scrape_metrics)"
  alerts_after="$(ch_count "SELECT count() FROM flinklab.vehicle_alerts WHERE alert_type='MATCH'")"
  metrics_after="$(ch_count "SELECT count() FROM flinklab.vehicle_window_metrics")"

  # 解析 scrape
  local lag_ms ckpt_ms restarts lag_w ckpt_w
  lag_ms="$(echo "${scrape}" | sed -n 's/^lag_vehicle_events_max_ms=//p')"
  ckpt_ms="$(echo "${scrape}" | sed -n 's/^ckpt_duration_max_ms=//p')"
  restarts="$(echo "${scrape}" | sed -n 's/^num_restarts_max=//p')"
  lag_w="$(echo "${scrape}" | sed -n 's/^lag_window_source_avg_ms=//p')"
  ckpt_w="$(echo "${scrape}" | sed -n 's/^ckpt_window_ms=//p')"

  local expected_events produced_eps
  expected_events=$((RATE * DURATION_SEC))
  if [[ "${wall_sec}" -gt 0 ]]; then
    produced_eps="$(python3 -c "print(round(${expected_events}/${wall_sec}, 2))")"
  else
    produced_eps="n/a"
  fi

  local flink_ver tms slots_total slots_avail jobs_running uname_line
  flink_ver="$(echo "${env_lines}" | sed -n 's/^flink_version=//p')"
  tms="$(echo "${env_lines}" | sed -n 's/^taskmanagers=//p')"
  slots_total="$(echo "${env_lines}" | sed -n 's/^slots_total=//p')"
  slots_avail="$(echo "${env_lines}" | sed -n 's/^slots_available=//p')"
  jobs_running="$(echo "${env_lines}" | sed -n 's/^jobs_running=//p')"
  uname_line="$(echo "${env_lines}" | sed -n 's/^uname=//p')"

  local alerts_delta metrics_delta
  if [[ -n "${alerts_before}" && -n "${alerts_after}" ]]; then
    alerts_delta=$((alerts_after - alerts_before))
  else
    alerts_delta="n/a"
  fi
  if [[ -n "${metrics_before}" && -n "${metrics_after}" ]]; then
    metrics_delta=$((metrics_after - metrics_before))
  else
    metrics_delta="n/a"
  fi

  mkdir -p "$(dirname "${BASELINE_MD}")"
  cat > "${BASELINE_MD}" <<EOF
# p03 项目级压测 baseline（OrbStack arm64 实测）

> 方法论子集对齐仓库 [\`benchmark/README.md\`](../../../benchmark/README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **非** P5 全矩阵；数字仅来自本次 \`loadtest.sh\` 运行，禁止当作生产 SLA。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | ${started_utc} |
| 主机 / 架构 | ${uname_line} |
| 运行时 | OrbStack + docker compose（\`docker/docker-compose.yml\`） |
| Flink | ${flink_ver}（镜像与仓库 SSOT 一致） |
| TaskManagers | ${tms} |
| Slots total / available | ${slots_total} / ${slots_avail} |
| Jobs running | ${jobs_running}（期望含 \`p03_vehicle_alert\` + \`p03_vehicle_window_metrics\`） |
| Kafka bootstrap（造数） | \`${BOOTSTRAP}\` |
| Prometheus | \`${PROM_URL}\` |
| 作业并行度快照 | 以 Flink UI / compose 当前配置为准（本报告不改并行度） |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | \`gen_vehicle_events.py --rate/--duration\`（无 k6/JMeter） |
| 配置速率 | ${RATE} events/s |
| 热身 | ${WARMUP_SEC}s（**丢弃**，不计入下表） |
| 计量时长 | ${DURATION_SEC}s（墙钟 ≈ ${wall_sec}s） |
| 期望发送量 | ${RATE} × ${DURATION_SEC} = ${expected_events} |
| vin | \`${VIN}\` × vin-count=${VIN_COUNT} |
| 事件混合 | ~80% HEARTBEAT / ~20% HARSH_ACCEL，advancing eventTime |
| 乱序 | 造数侧不额外注入乱序；作业 ooo=5s |

## 3. 指标

吞吐口径：配置速率 ${RATE} eps；墙钟折算发送速率 ≈ **${produced_eps}** eps（期望发送量 / 计量墙钟秒）。
lag/checkpoint/restarts 来自 Prometheus instant query（\`job_name=~"p03.*"\`）；CH 行为辅助观测。

| 指标 | 值 | 来源 |
|---|---|---|
| 配置 produce rate | ${RATE} eps | loadtest 参数 |
| 墙钟折算 produce rate | ${produced_eps} eps | ${expected_events} / ${wall_sec}s |
| currentEmitEventTimeLag（vehicle_events source，max） | ${lag_ms} ms | PromQL |
| lastCheckpointDuration（p03 jobs，max） | ${ckpt_ms} ms | PromQL |
| numRestarts（p03 jobs，max） | ${restarts} | PromQL |
| window job source lag（avg） | ${lag_w} ms | PromQL |
| window job lastCheckpointDuration | ${ckpt_w} ms | PromQL |
| CH MATCH 增量（meter 前后） | ${alerts_delta}（${alerts_before} → ${alerts_after}） | ClickHouse |
| CH vehicle_window_metrics 增量 | ${metrics_delta}（${metrics_before} → ${metrics_after}） | ClickHouse |

## 4. 结论

- 本轮在 OrbStack arm64 上以 **${RATE} eps × ${DURATION_SEC}s**（热身 ${WARMUP_SEC}s 已丢弃）跑通项目级压测，并留下上表实测数字。
- Checkpoint 最大耗时 **${ckpt_ms} ms**；p03 作业 \`numRestarts\` max=**${restarts}**（若 >0 须在复盘中解释）。
- Event-time lag（vehicle_events source max）= **${lag_ms} ms**；窗口作业 source lag avg=**${lag_w} ms**。
- 复跑：\`cd projects/p03-vehicle-monitoring && make loadtest\`（可用 \`RATE\`/\`WARMUP_SEC\`/\`DURATION_SEC\` 覆盖）。
- 全矩阵 / 更高 eps / 倾斜实验留给 P5 \`benchmark/\`（PROD-01），本文件不扩展。

EOF

  echo "ok wrote ${BASELINE_MD}"
  echo "${scrape}"
  echo "alerts_match ${alerts_before} → ${alerts_after} (delta=${alerts_delta})"
  echo "window_metrics ${metrics_before} → ${metrics_after} (delta=${metrics_delta})"
}

main "$@"
