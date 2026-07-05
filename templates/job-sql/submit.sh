#!/usr/bin/env bash
# 标准提交脚本:先出 EXPLAIN 存档,再正式执行。
# 用法:bash submit.sh(在容器内或已配置 SQL Client 的环境执行)
set -euo pipefail
cd "$(dirname "$0")"

STAMP=$(date +%Y%m%d-%H%M%S)
mkdir -p explain

echo "== 生成 EXPLAIN 存档(evidence for review)=="
{
  cat ddl/*.sql
  echo "EXPLAIN $(grep -A100 'BEGIN STATEMENT SET' statements/main.sql | grep -B100 'END;' | grep -v 'BEGIN STATEMENT SET\|END;');"
} | sql-client.sh -f /dev/stdin > "explain/${STAMP}.txt" || true
echo "已生成 explain/${STAMP}.txt(请人工检查后再执行下一步)"

echo "== 正式提交(DDL + 业务语句)=="
cat ddl/*.sql statements/main.sql | sql-client.sh -f /dev/stdin
