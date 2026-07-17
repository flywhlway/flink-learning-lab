#!/usr/bin/env bash
# p03 VEH-02 验收：ClickHouse flinklab.vehicle_alerts count 为唯一放行条件。
# Kafka vehicle.alerts 仅作可选诊断日志，不得单独放行。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "FAIL: 未找到 ${COMPOSE_FILE}" >&2
  exit 1
fi

# 固定表名，禁止拼接外部输入（T-1-01）
CH_QUERY='SELECT count() FROM flinklab.vehicle_alerts'

if ! COUNT="$(docker compose -f "${COMPOSE_FILE}" exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "${CH_QUERY}" 2>/dev/null)"; then
  echo "FAIL: 无法查询 flinklab.vehicle_alerts（ClickHouse 未起或表不存在）" >&2
  # 可选诊断：Kafka 有消息也不能放行
  echo "diag: Kafka vehicle.alerts 仅供排障，不作为放行条件" >&2
  exit 1
fi

COUNT="$(printf '%s' "${COUNT}" | tr -d '[:space:]')"
if [[ -z "${COUNT}" || "${COUNT}" -lt 1 ]]; then
  echo "FAIL: expected vehicle_alerts rows >= 1, got ${COUNT:-0}" >&2
  echo "diag: Kafka vehicle.alerts 仅供排障，不作为放行条件" >&2
  exit 1
fi

echo "ok alerts_rows=${COUNT}"
