#!/usr/bin/env bash
# p01 LOG-03 可选 AI 验收入口（D-06）：
#   前置：宿主机 Ollama 可用 + 作业 --ai.enabled=true
#   权威出口：ClickHouse flinklab.log_results 中 ai_source=AI（及可观察 ai_risk）
#   主 CI/Phase 门禁仍以 verify.sh（规则路径）为准；本脚本为本机可选硬验收。
#
# Wave 0 骨架：默认非 0。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"

if [[ "${1:-}" != "--implemented" ]]; then
  echo "FAIL: Wave0 stub — verify_ai.sh 要求 Ollama + ai_source=AI 断言，尚未实现" >&2
  echo "hint: OLLAMA_URL=${OLLAMA_URL}；完成后: bash scripts/verify_ai.sh --implemented" >&2
  exit 1
fi

echo "FAIL: Wave0 stub — --implemented 路径尚未接线（Ollama / ai_source / ai_risk）" >&2
exit 1
