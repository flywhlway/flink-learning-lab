#!/usr/bin/env bash
# VEH-01：断言 p03 profile 隔离——default compose 不含 p03-init，--profile p03 含之。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DOCKER_DIR="${REPO_ROOT}/docker"

if [[ ! -d "${DOCKER_DIR}" ]]; then
  echo "FAIL: 未找到 ${DOCKER_DIR}" >&2
  exit 1
fi

cd "${DOCKER_DIR}"

echo "==> docker compose config -q（默认）"
docker compose config -q

echo "==> docker compose --profile p03 config -q"
docker compose --profile p03 config -q

echo "==> 断言默认服务列表不含 p03-init"
DEFAULT_SERVICES="$(docker compose config --services)"
if printf '%s\n' "${DEFAULT_SERVICES}" | grep -qx 'p03-init'; then
  echo "FAIL: 无 profile 时服务列表不应包含 p03-init" >&2
  echo "${DEFAULT_SERVICES}" >&2
  exit 1
fi

echo "==> 断言 --profile p03 服务列表含 p03-init"
P03_SERVICES="$(docker compose --profile p03 config --services)"
if ! printf '%s\n' "${P03_SERVICES}" | grep -qx 'p03-init'; then
  echo "FAIL: --profile p03 时服务列表必须包含 p03-init" >&2
  echo "${P03_SERVICES}" >&2
  exit 1
fi

echo "ok smoke_profile: default 无 p03-init；--profile p03 有 p03-init"
