#!/usr/bin/env bash
# p02 compose profile 隔离冒烟（D-08）：无 profile 不含 p02-init；--profile p02 含之。
# 配置级断言必过；若 OrbStack 基座可达则再验 up-p02 / topics / reco_items（不假绿）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${ROOT}/docker"

cd "${COMPOSE_DIR}"

echo "==> smoke_p02_profile: docker compose config -q"
docker compose config -q

echo "==> smoke_p02_profile: 无 profile 服务列表不得含 p02-init"
DEFAULT_SERVICES="$(docker compose config --services)"
if echo "${DEFAULT_SERVICES}" | grep -qx 'p02-init'; then
  echo "FAIL: default compose services 含 p02-init，污染 make up（D-08）" >&2
  echo "${DEFAULT_SERVICES}" >&2
  exit 1
fi

echo "==> smoke_p02_profile: --profile p02 服务列表必须含 p02-init"
P02_SERVICES="$(docker compose --profile p02 config --services)"
if ! echo "${P02_SERVICES}" | grep -qx 'p02-init'; then
  echo "FAIL: --profile p02 缺少 p02-init" >&2
  echo "${P02_SERVICES}" >&2
  exit 1
fi

# default make up 目标不得带 --profile p02
if grep -E '^up:' -A5 "${COMPOSE_DIR}/Makefile" | grep -q -- '--profile p02'; then
  echo "FAIL: docker/Makefile up 目标含 --profile p02" >&2
  exit 1
fi

# 解析 p02-init 的 depends_on：仅允许 kafka/clickhouse（与 p01 纪律一致）
P02_DEPS="$(docker compose --profile p02 config 2>/dev/null | awk '
  /^  p02-init:/ {in_svc=1; next}
  in_svc && /^  [a-zA-Z0-9_-]+:/ {in_svc=0}
  in_svc && /depends_on:/ {in_dep=1; next}
  in_svc && in_dep && /^      [a-zA-Z0-9_-]+:/ {
    gsub(/^[[:space:]]+/, "", $1); gsub(/:/, "", $1); print $1
  }
  in_svc && in_dep && /^    [a-zA-Z]/ {in_dep=0}
')"
if [ -n "${P02_DEPS}" ]; then
  while IFS= read -r dep; do
    case "${dep}" in
      kafka|clickhouse) ;;
      *)
        echo "FAIL: p02-init depends_on 含未预期依赖: ${dep}" >&2
        exit 1
        ;;
    esac
  done <<< "${P02_DEPS}"
fi

echo "OK: smoke_p02_profile config — default 无 p02-init；--profile p02 有 p02-init；up 未污染"

# --- 运行时可达性（OrbStack）：基座未 up 则 FAIL，禁止假绿 ---
kafka_up=0
postgres_up=0
if docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qE 'fll-kafka|kafka'; then
  kafka_up=1
fi
if docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qE 'fll-postgres|postgres'; then
  postgres_up=1
fi

if [[ "${kafka_up}" -ne 1 || "${postgres_up}" -ne 1 ]]; then
  echo "FAIL: 基座未完整 up（需要 kafka + postgres 运行中）— 请先: cd docker && make up" >&2
  echo "hint: redis 亦应在 default up 中可达，供后续 05-02 特征通道使用" >&2
  exit 1
fi

echo "==> smoke_p02_profile: make up-p02（幂等）"
make up-p02

echo "==> smoke_p02_profile: 等待 p02-init 完成"
deadline=$(( $(date +%s) + 120 ))
while true; do
  st="$(docker compose --profile p02 ps -a --format '{{.Name}} {{.Status}}' 2>/dev/null | grep p02-init || true)"
  if echo "${st}" | grep -qiE 'Exited \(0\)|exit 0'; then
    echo "p02-init exit 0: ${st}"
    break
  fi
  if echo "${st}" | grep -qiE 'Exited \([1-9]|exit [1-9]'; then
    echo "FAIL: p02-init 非 0 退出: ${st}" >&2
    docker compose --profile p02 logs p02-init 2>&1 | tail -40 >&2 || true
    exit 1
  fi
  now=$(date +%s)
  if [[ "${now}" -ge "${deadline}" ]]; then
    echo "FAIL: 等待 p02-init 超时: ${st}" >&2
    exit 1
  fi
  sleep 2
done

echo "==> smoke_p02_profile: Kafka topics 含 reco.events"
TOPICS="$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || true)"
if ! echo "${TOPICS}" | grep -qx 'reco.events'; then
  echo "FAIL: topic 列表缺少 reco.events" >&2
  echo "${TOPICS}" >&2
  exit 1
fi
if ! echo "${TOPICS}" | grep -qx 'reco.results'; then
  echo "FAIL: topic 列表缺少 reco.results" >&2
  echo "${TOPICS}" >&2
  exit 1
fi

echo "==> smoke_p02_profile: PG reco_items 行数 ≥ 50"
COUNT="$(docker compose exec -T postgres \
  psql -U "${PG_USER:-flinklab}" -d flinklab -tAc 'SELECT count(*) FROM reco_items;' 2>/dev/null | tr -d '[:space:]')"
if ! [[ "${COUNT}" =~ ^[0-9]+$ ]] || [[ "${COUNT}" -lt 50 ]]; then
  echo "FAIL: reco_items count=${COUNT:-empty}（期望 ≥50）" >&2
  exit 1
fi

echo "OK: smoke_p02_profile — topics reco.events/reco.results 已创建；reco_items=${COUNT}"
exit 0
