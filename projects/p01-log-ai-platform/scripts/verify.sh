#!/usr/bin/env bash
# p01 LOG-01/02 验收入口（D-06/D-10）：
#   权威出口 = ClickHouse flinklab.log_results（rule_label 白名单过滤）
#   Kafka logs.events / 下游 topic 仅诊断，不得单独 exit 0
#
# Wave 0 骨架：默认非 0。后续切片实现 CH 查询与 MIN_COUNT 放行后，
# 须带 --implemented 才走绿路径；无作业/空表仍非 0。
#
# 环境变量（规划）：
#   RULE_LABEL  白名单 rule_label，默认 AUTH_FAIL（T-04-01）
#   MIN_COUNT   行数下限，默认 1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

RULE_LABEL="${RULE_LABEL:-AUTH_FAIL}"
MIN_COUNT="${MIN_COUNT:-1}"

# T-04-01：仅允许白名单枚举拼入 SQL；拒绝任意环境变量注入
case "${RULE_LABEL}" in
  AUTH_FAIL|ERROR_BURST|NONE)
    ;;
  *)
    echo "FAIL: RULE_LABEL 非法（仅允许 AUTH_FAIL|ERROR_BURST|NONE），got=${RULE_LABEL}" >&2
    exit 1
    ;;
esac

if [[ "${1:-}" != "--implemented" ]]; then
  echo "FAIL: Wave0 stub — verify.sh 尚未实现 CH 权威查询（flinklab.log_results WHERE rule_label='${RULE_LABEL}'）" >&2
  echo "hint: 后续切片完成后使用: bash scripts/verify.sh --implemented" >&2
  exit 1
fi

# 占位：完整实现将查询 SELECT count() FROM flinklab.log_results WHERE rule_label='...'
# Kafka 仅 diag，禁止作为放行条件。
echo "FAIL: Wave0 stub — --implemented 路径尚未接线（log_results / rule_label）" >&2
exit 1
