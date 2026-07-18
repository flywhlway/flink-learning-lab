#!/usr/bin/env bash
# p02 RECO-03 / D-11 压测入口（项目级，非 benchmark/ 全矩阵）：
#   gen --rate/--duration → 刮 Prometheus / ClickHouse → 写 docs/baseline.md
# 禁止 k6/JMeter；权威观测仍以 CH reco_results 为准；Kafka/Redis 仅诊断。
# 数字仅 OrbStack arm64 实测；禁止编造。
# T-05-03：RATE 封顶 MAX_RATE；仅本地 lab。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
P02_DIR="${REPO_ROOT}/projects/p02-realtime-reco"
BASELINE_MD="${P02_DIR}/docs/baseline.md"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

PROM_URL="${PROM_URL:-http://localhost:9090}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
RATE="${RATE:-100}"
WARMUP_SEC="${WARMUP_SEC:-30}"
DURATION_SEC="${DURATION_SEC:-90}"
MAX_RATE="${MAX_RATE:-500}"
LOADTEST_USER="${LOADTEST_USER:-u-loadtest}"

if ! [[ "${RATE}" =~ ^[0-9]+$ ]] || [[ "${RATE}" -lt 1 ]]; then
  echo "FAIL: RATE 须为正整数，got=${RATE}" >&2
  exit 1
fi
if [[ "${RATE}" -gt "${MAX_RATE}" ]]; then
  echo "FAIL: RATE=${RATE} 超过 lab 封顶 MAX_RATE=${MAX_RATE}（T-05-03）" >&2
  exit 1
fi

prom_query() {
  local q="$1"
  curl -sfG "${PROM_URL}/api/v1/query" --data-urlencode "query=${q}" 2>/dev/null \
    || return 1
}

prom_scalar_max() {
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
  if ! docker compose -f "${COMPOSE_FILE}" exec -T jobmanager flink list -r 2>/dev/null \
    | grep -q 'p02-realtime-reco'; then
    echo "FAIL: 无 RUNNING p02-realtime-reco；请先 make submit / make match" >&2
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
    echo "FAIL: Redis 不可达（loadtest 期望基座 redis 健康；勿与 drill 交叉污染）" >&2
    exit 1
  fi
  if ! python3 -c "
from confluent_kafka.admin import AdminClient
c=AdminClient({'bootstrap.servers':'${BOOTSTRAP}'})
md=c.list_topics(timeout=5)
assert md is not None
" 2>/dev/null; then
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

# 运行造数；stdout 最后一行含 events=N；经命名管道变量返回实际发送量
run_gen() {
  local dur="$1"
  local label="$2"
  local out events
  echo "==> gen rate=${RATE} duration=${dur}s (${label}) bootstrap=${BOOTSTRAP} user=${LOADTEST_USER}"
  out="$(uv run "${SCRIPT_DIR}/gen_reco_events.py" \
    --bootstrap "${BOOTSTRAP}" \
    --rate "${RATE}" \
    --duration "${dur}" \
    --user "${LOADTEST_USER}" 2>&1)" || {
    echo "${out}" >&2
    return 1
  }
  echo "${out}"
  events="$(printf '%s\n' "${out}" | sed -n 's/.*events=\([0-9][0-9]*\).*/\1/p' | tail -1)"
  if [[ -z "${events}" ]]; then
    echo "FAIL: 无法从 gen 输出解析 events=N" >&2
    return 1
  fi
  LAST_GEN_EVENTS="${events}"
}

scrape_metrics() {
  local lag_ms ckpt_ms restarts
  lag_ms="$(prom_scalar_max \
    'flink_taskmanager_job_task_operator_currentEmitEventTimeLag{job_name=~"p02.*",operator_name="Source:_kafka_reco_events"}' \
    || true)"
  ckpt_ms="$(prom_scalar_max \
    'flink_jobmanager_job_lastCheckpointDuration{job_name=~"p02.*"}' \
    || true)"
  restarts="$(prom_scalar_max \
    'flink_jobmanager_job_numRestarts{job_name=~"p02.*"}' \
    || true)"

  echo "lag_reco_events_max_ms=${lag_ms:-n/a}"
  echo "ckpt_duration_max_ms=${ckpt_ms:-n/a}"
  echo "num_restarts_max=${restarts:-n/a}"
}

main() {
  echo "p02 loadtest: rate=${RATE} warmup=${WARMUP_SEC}s measure=${DURATION_SEC}s"
  echo "note: 非生产压测；仅打 lab Kafka（${BOOTSTRAP}）；MAX_RATE=${MAX_RATE}"
  require_deps

  local started_utc env_lines
  started_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  env_lines="$(snapshot_env)"

  local rows_before
  rows_before="$(ch_count "SELECT count() FROM flinklab.reco_results")"

  local LAST_GEN_EVENTS=""
  echo "==> warmup ${WARMUP_SEC}s（指标丢弃，不计入 baseline）"
  run_gen "${WARMUP_SEC}" "warmup"
  local warmup_events="${LAST_GEN_EVENTS}"
  sleep 5

  echo "==> measure ${DURATION_SEC}s"
  local measure_start measure_end
  measure_start="$(date +%s)"
  run_gen "${DURATION_SEC}" "measure"
  measure_end="$(date +%s)"
  local wall_sec=$((measure_end - measure_start))
  local measure_events="${LAST_GEN_EVENTS}"

  # 给 Top-K / Sink 落地时间
  sleep 15

  local scrape rows_after
  scrape="$(scrape_metrics)"
  rows_after="$(ch_count "SELECT count() FROM flinklab.reco_results")"

  local lag_ms ckpt_ms restarts
  lag_ms="$(echo "${scrape}" | sed -n 's/^lag_reco_events_max_ms=//p')"
  ckpt_ms="$(echo "${scrape}" | sed -n 's/^ckpt_duration_max_ms=//p')"
  restarts="$(echo "${scrape}" | sed -n 's/^num_restarts_max=//p')"

  local configured_events produced_eps ch_rows_per_s
  configured_events=$((RATE * DURATION_SEC))
  if [[ "${wall_sec}" -gt 0 && -n "${measure_events}" ]]; then
    produced_eps="$(python3 -c "print(round(${measure_events}/${wall_sec}, 2))")"
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

  local rows_delta
  if [[ -n "${rows_before}" && -n "${rows_after}" && "${rows_before}" =~ ^[0-9]+$ && "${rows_after}" =~ ^[0-9]+$ ]]; then
    rows_delta=$((rows_after - rows_before))
  else
    rows_delta="n/a"
  fi
  if [[ "${rows_delta}" =~ ^[0-9]+$ ]] && [[ "${wall_sec}" -gt 0 ]]; then
    # 含热身后的总增量粗速率；计量段近似用 wall_sec（热身未单独扣 CH）
    ch_rows_per_s="$(python3 -c "print(round(${rows_delta}/max(${wall_sec}+${WARMUP_SEC}+20,1), 2))")"
  else
    ch_rows_per_s="n/a"
  fi

  mkdir -p "$(dirname "${BASELINE_MD}")"
  cat > "${BASELINE_MD}" <<EOF
# p02 项目级压测 baseline（OrbStack arm64 实测）

> 方法论子集对齐仓库 [\`benchmark/README.md\`](../../../benchmark/README.md)：环境快照 → 负载定义 → 指标表 → 结论。
> **非** P5 全矩阵；数字仅来自本次 \`loadtest.sh\` 运行，禁止当作生产 SLA。
> 负载仅打 lab Kafka（\`${BOOTSTRAP}\`），禁止当作生产压测工具链。

## 1. 环境

| 项 | 值 |
|---|---|
| 采集时间 (UTC) | ${started_utc} |
| 主机 / 架构 | ${uname_line} |
| 运行时 | OrbStack + docker compose（\`docker/docker-compose.yml\`） |
| Flink | ${flink_ver}（镜像与仓库 SSOT 一致） |
| TaskManagers | ${tms} |
| Slots total / available | ${slots_total} / ${slots_avail} |
| Jobs running | ${jobs_running}（期望含 \`p02-realtime-reco\`） |
| Kafka bootstrap（造数） | \`${BOOTSTRAP}\` |
| Prometheus | \`${PROM_URL}\` |
| 作业并行度快照 | 以 Flink UI / compose 当前配置为准（本报告不改并行度） |

## 2. 负载

| 项 | 值 |
|---|---|
| 驱动 | \`gen_reco_events.py --rate/--duration\`（无 k6/JMeter） |
| 配置速率 | ${RATE} events/s |
| 热身 | ${WARMUP_SEC}s（实际发送 ${warmup_events} 条；**丢弃**，不计入下表吞吐主数字） |
| 计量时长 | ${DURATION_SEC}s（墙钟 ≈ ${wall_sec}s） |
| 配置期望发送量 | ${RATE} × ${DURATION_SEC} = ${configured_events} |
| 计量段实际发送量 | ${measure_events}（\`gen_reco_events.py\` 回报） |
| userId | \`${LOADTEST_USER}\` |
| 事件混合 | VIEW/CLICK/CART/BUY 轮转 + 种子 item \`i-001\`..\`i-050\` |
| 封顶 | RATE ≤ MAX_RATE=${MAX_RATE}（T-05-03，仅本地 lab） |

## 3. 指标

吞吐口径：配置速率 ${RATE} eps；墙钟折算**实测**发送速率 ≈ **${produced_eps}** eps（计量段实际发送量 / 计量墙钟秒）。
lag/checkpoint/restarts 来自 Prometheus instant query（\`job_name=~"p02.*"\`）；CH 行为辅助观测。
说明：每条行为经 Top-K=5 可写出多行 \`reco_results\`，故 CH 行增量通常 **高于** 事件数。

| 指标 | 值 | 来源 |
|---|---|---|
| 配置 produce rate | ${RATE} eps | loadtest 参数 |
| 墙钟折算 produce rate（实测） | ${produced_eps} eps | ${measure_events} / ${wall_sec}s |
| currentEmitEventTimeLag（kafka_reco_events source，max） | ${lag_ms} ms | PromQL |
| lastCheckpointDuration（p02 jobs，max） | ${ckpt_ms} ms | PromQL |
| numRestarts（p02 jobs，max） | ${restarts} | PromQL |
| CH reco_results 增量（warmup+measure 前后） | ${rows_delta}（${rows_before} → ${rows_after}） | ClickHouse |
| CH 粗写入速率（增量 / 墙钟近似） | ${ch_rows_per_s} 行/s | ClickHouse |

## 4. 结论

- 本轮在 OrbStack arm64 上以 **${RATE} eps × ${DURATION_SEC}s**（热身 ${WARMUP_SEC}s 已丢弃于吞吐主数字）跑通项目级压测，并留下上表实测数字。
- Checkpoint 最大耗时 **${ckpt_ms} ms**；p02 作业 \`numRestarts\` max=**${restarts}**（若 >0 须在复盘中解释）。
- Event-time lag（kafka_reco_events source max）= **${lag_ms} ms**。
- 复跑：\`cd projects/p02-realtime-reco && make loadtest\`（可用 \`RATE\`/\`WARMUP_SEC\`/\`DURATION_SEC\` 覆盖）。
- 全矩阵 / 更高 eps / 倾斜实验留给 P5 \`benchmark/\`（PROD-01），本文件不扩展。
- 恰好 2 条硬演练：本 loadtest + \`make drill-redis\`（D-12）；杀 TM / 断 Kafka 不挡完成。

EOF

  echo "ok wrote ${BASELINE_MD}"
  echo "${scrape}"
  echo "reco_results ${rows_before} → ${rows_after} (delta=${rows_delta})"
}

main "$@"
