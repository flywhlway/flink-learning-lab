#!/usr/bin/env bash
# ENG-01…04 终检（D-10）。用法:bash scripts/eng_audit.sh
# 失败非 0。ENG-04：CHANGELOG Unreleased + PHASES P6 可验证完成态严格断言（07-03）。
set -uo pipefail
cd "$(dirname "$0")/.."
FAIL=0
note() { printf '%s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; FAIL=1; }
ok()   { printf 'ok    %s\n' "$*"; }

# ── ENG-01: 根 README 版本矩阵关键版本 ⊆ examples/pom.xml <properties> ──
# 抽样: 2.2.1 / 5.0.0-2.2 / 3.6.0 / 0.3.0
# README-only 白名单（不要求进 pom）: Operator Helm chart / Milvus / Ollama / Flink Kubernetes Operator
note "── ENG-01 版本抽样 ──"
PROPS=$(sed -n '/<properties>/,/<\/properties>/p' examples/pom.xml)
ENG01_OK=1
for ver in '2.2.1' '5.0.0-2.2' '3.6.0' '0.3.0'; do
  if printf '%s\n' "$PROPS" | grep -Fq "$ver"; then
    ok "ENG-01 pom properties 含 $ver"
  else
    bad "ENG-01 examples/pom.xml <properties> 缺少版本串 $ver"
    ENG01_OK=0
  fi
done
# 白名单组件须在 README 版本矩阵出现（文档声明即可）
for label in 'Flink Kubernetes Operator' 'Milvus' 'Ollama'; do
  if grep -Fq "$label" README.md; then
    ok "ENG-01 README-only 白名单已声明: $label"
  else
    bad "ENG-01 README 未声明白名单组件: $label"
    ENG01_OK=0
  fi
done
[ "$ENG01_OK" -eq 1 ] && ok "ENG-01 版本抽样通过"

# ── ENG-02: docs/README.md + 模块 00–11/13/14 目录 + 模块 15 三项目 README ──
note "── ENG-02 文档编号 ──"
ENG02_OK=1
if [ -f docs/README.md ]; then
  ok "ENG-02 docs/README.md 存在"
else
  bad "ENG-02 缺少 docs/README.md"
  ENG02_OK=0
fi
for d in \
  docs/00-landscape docs/01-runtime docs/02-time-window docs/03-state \
  docs/04-checkpoint docs/05-sql docs/06-table-api docs/07-connectors \
  docs/08-cdc docs/09-lakehouse docs/10-cep docs/11-ecosystem \
  docs/13-performance docs/14-production
do
  if [ -d "$d" ]; then
    ok "ENG-02 模块目录 $d"
  else
    bad "ENG-02 缺少模块目录 $d"
    ENG02_OK=0
  fi
done
for proj in \
  projects/p01-log-ai-platform/README.md \
  projects/p02-realtime-reco/README.md \
  projects/p03-vehicle-monitoring/README.md
do
  if [ -f "$proj" ]; then
    ok "ENG-02 模块 15 链接目标 $proj"
  else
    bad "ENG-02 模块 15 链接不可解析: $proj"
    ENG02_OK=0
  fi
done
[ "$ENG02_OK" -eq 1 ] && ok "ENG-02 文档编号通过"

# ── ENG-03: 关键路径硬存在（verify/baseline 证据指针）──
note "── ENG-03 证据指针 ──"
ENG03_OK=1
for f in \
  projects/p01-log-ai-platform/docs/baseline.md \
  projects/p02-realtime-reco/docs/baseline.md \
  projects/p03-vehicle-monitoring/docs/baseline.md \
  benchmark/baseline.md \
  production/docs/bluegreen-timeline.md
do
  if [ -f "$f" ]; then
    ok "ENG-03 证据 $f"
  else
    bad "ENG-03 缺少证据指针 $f"
    ENG03_OK=0
  fi
done
# 违禁词子集抽检（与 qa_check ② 同词表；排除集同 D-08）
HITS=$(grep -rn --include='*.md' --include='*.java' --include='*.py' \
        -E '(TODO|FIXME|自行实现|请参考官网|省略)' \
        projects/p01-log-ai-platform projects/p02-realtime-reco \
        projects/p03-vehicle-monitoring benchmark production \
      | grep -v -E '(\./\.planning/|PHASES\.md|CLAUDE\.md|AGENTS\.md|scripts/README\.md|scripts/qa_check\.sh|docs/README\.md)' || true)
if [ -n "$HITS" ]; then
  bad "ENG-03 证据路径含违禁词:"
  printf '%s\n' "$HITS"
  ENG03_OK=0
else
  ok "ENG-03 证据路径违禁词抽检"
fi
[ "$ENG03_OK" -eq 1 ] && ok "ENG-03 证据指针通过"

# ── ENG-04: CHANGELOG Unreleased + PHASES P6 可验证完成态（严格）──
note "── ENG-04 状态痕迹 ──"
ENG04_OK=1
if grep -Eq '\[Unreleased\]|未发布' CHANGELOG.md; then
  ok "ENG-04 CHANGELOG 含 Unreleased/未发布区"
else
  bad "ENG-04 CHANGELOG.md 缺少 Unreleased/未发布区"
  ENG04_OK=0
fi
P6_LINE=$(grep -E '\*\*P6 总装 QA\*\*' PHASES.md | head -1 || true)
if [ -z "$P6_LINE" ]; then
  bad "ENG-04 PHASES.md 缺少 **P6 总装 QA** 行"
  ENG04_OK=0
elif echo "$P6_LINE" | grep -q '📋' && ! echo "$P6_LINE" | grep -Fq '可验证完成态'; then
  bad "ENG-04 PHASES P6 仍为 📋 占位，未达可验证完成态"
  ENG04_OK=0
elif echo "$P6_LINE" | grep -Fq '可验证完成态' \
  && echo "$P6_LINE" | grep -Eq 'QA-01' \
  && echo "$P6_LINE" | grep -Eq '100' \
  && echo "$P6_LINE" | grep -Eq '30000|30k|30k 行|≥30k'; then
  ok "ENG-04 PHASES P6 可验证完成态（含 QA-01 / 100 / 30000 口径）"
else
  bad "ENG-04 PHASES P6 缺少可验证完成态表述（须含可验证完成态 + QA-01 + mains≥100 + md≥30000）"
  note "info  ENG-04 P6 行: ${P6_LINE:-"(空)"}"
  ENG04_OK=0
fi
# 额外：Unreleased 应出现 P6 / qa_check / eng_audit 痕迹（发布说明草稿，D-12 不打 tag）
if grep -E 'P6|qa_check|eng_audit' CHANGELOG.md | head -1 >/dev/null; then
  ok "ENG-04 CHANGELOG 含 P6/qa_check/eng_audit 草稿痕迹"
else
  bad "ENG-04 CHANGELOG 缺少 P6/qa_check/eng_audit 发布说明草稿"
  ENG04_OK=0
fi
[ "$ENG04_OK" -eq 1 ] && ok "ENG-04 状态痕迹通过"

if [ "$FAIL" -eq 0 ]; then
  note "== ENG AUDIT PASS =="
else
  note "== ENG AUDIT FAIL =="
  exit 1
fi
