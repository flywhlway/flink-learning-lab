#!/usr/bin/env bash
# p01 compose profile 隔离冒烟（D-08）：无 profile 不含 p01-init；--profile p01 含之。
# 配置级断言即可放行；不要求本机已 up 基座。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${ROOT}/docker"

cd "${COMPOSE_DIR}"

echo "==> smoke_p01_profile: docker compose config -q"
docker compose config -q

echo "==> smoke_p01_profile: 无 profile 服务列表不得含 p01-init"
DEFAULT_SERVICES="$(docker compose config --services)"
if echo "${DEFAULT_SERVICES}" | grep -qx 'p01-init'; then
  echo "FAIL: default compose services 含 p01-init，污染 make up（D-08）" >&2
  echo "${DEFAULT_SERVICES}" >&2
  exit 1
fi

echo "==> smoke_p01_profile: --profile p01 服务列表必须含 p01-init"
P01_SERVICES="$(docker compose --profile p01 config --services)"
if ! echo "${P01_SERVICES}" | grep -qx 'p01-init'; then
  echo "FAIL: --profile p01 缺少 p01-init" >&2
  echo "${P01_SERVICES}" >&2
  exit 1
fi

echo "==> smoke_p01_profile: --profile p01 服务列表不得含 milvus"
if echo "${P01_SERVICES}" | grep -qiE '^milvus'; then
  echo "FAIL: --profile p01 服务列表出现 milvus（禁止绑进 p01）" >&2
  exit 1
fi

# 解析 p01-init 的 depends_on：仅允许 kafka/clickhouse
P01_DEPS="$(docker compose --profile p01 config 2>/dev/null | awk '
  /^  p01-init:/ {in_svc=1; next}
  in_svc && /^  [a-zA-Z0-9_-]+:/ {in_svc=0}
  in_svc && /depends_on:/ {in_dep=1; next}
  in_svc && in_dep && /^      [a-zA-Z0-9_-]+:/ {
    gsub(/^[[:space:]]+/, "", $1); gsub(/:/, "", $1); print $1
  }
  in_svc && in_dep && /^    [a-zA-Z]/ {in_dep=0}
')"
if [ -n "${P01_DEPS}" ]; then
  while IFS= read -r dep; do
    case "${dep}" in
      kafka|clickhouse) ;;
      *)
        echo "FAIL: p01-init depends_on 含未预期依赖: ${dep}" >&2
        exit 1
        ;;
    esac
  done <<< "${P01_DEPS}"
fi

# default make up 目标不得带 --profile p01
if grep -E '^up:' -A5 "${COMPOSE_DIR}/Makefile" | grep -q -- '--profile p01'; then
  echo "FAIL: docker/Makefile up 目标含 --profile p01" >&2
  exit 1
fi

echo "OK: smoke_p01_profile — default 无 p01-init；--profile p01 有 p01-init；up 未污染"
exit 0
