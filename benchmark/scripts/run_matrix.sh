#!/usr/bin/env bash
# 仓库级压测矩阵入口（PROD-01 / D-01 / D-02）。
# Wave 0：默认失败态——未传 --implemented 时 FAIL 非 0。
# 驱动锁定既有 Python 造数 / 项目 gen + 本脚本包装（D-02：不引入未登记压测镜像）。
# 目标产物：benchmark/baseline.md（本 Wave 可不生成文件）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BASELINE_MD="${REPO_ROOT}/benchmark/baseline.md"
PROM_URL="${PROM_URL:-http://localhost:9090}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
EPS="${EPS:-1000}"
DRY_RUN="${DRY_RUN:-0}"
IMPLEMENTED=0

usage() {
  cat <<'EOF'
用法: bash benchmark/scripts/run_matrix.sh [--dry-run] [--implemented] [--eps N]

环境变量:
  EPS       负载轴 eps（默认 1000；亦可用 --eps）
  DRY_RUN   设为 1 等同 --dry-run
  PROM_URL  Prometheus（默认 http://localhost:9090）
  FLINK_URL Flink REST（默认 http://localhost:8081）

Wave 0 门禁: 未传 --implemented 时打印 FAIL 并 exit 1。
完整矩阵跑通后须写入 benchmark/baseline.md。
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --implemented) IMPLEMENTED=1; shift ;;
    --eps)
      EPS="${2:?--eps 需要数值}"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "FAIL: 未知参数 $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${DRY_RUN}" == "1" ]] || [[ "${DRY_RUN}" == "true" ]]; then
  echo "==> dry-run: EPS=${EPS} PROM_URL=${PROM_URL} FLINK_URL=${FLINK_URL}"
  echo "==> 将写入目标: ${BASELINE_MD}（完整矩阵实现后生成）"
fi

if [[ "${IMPLEMENTED}" -ne 1 ]]; then
  echo "FAIL: 压测矩阵尚未实现完整跑通路径（缺少 --implemented）" >&2
  echo "FAIL: 预期产物 ${BASELINE_MD} 未由本 harness 实测生成；禁止假绿" >&2
  echo "info: 作业轴=e01-J2+e10+p03；负载轴=1k/5k eps（20k stretch）；见 CONTEXT D-01" >&2
  exit 1
fi

# 后续 wave：--implemented 后走 warmup→measure→scrape→baseline
if [[ ! -s "${BASELINE_MD}" ]]; then
  echo "FAIL: --implemented 已设但 ${BASELINE_MD} 不存在或为空" >&2
  exit 1
fi

echo "ok matrix baseline=${BASELINE_MD} eps=${EPS}"
