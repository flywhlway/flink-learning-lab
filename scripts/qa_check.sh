#!/usr/bin/env bash
# 仓库 QA 自检(PHASES.md 约定的验收工具)。用法:bash scripts/qa_check.sh
# 五硬门:① compose YAML 可解析 ② 违禁词扫描 ③ Markdown 相对链接存在性
#         ④ 可运行案例计数 ≥100 ⑤ Maven 全模块编译(硬失败)
# 文档行数仅诊断打印（不再作为硬门禁；见 .planning/MEMORY.md 2026-07-19 注水整改）
# 末尾可选调用 scripts/eng_audit.sh(ENG-01…04)
set -uo pipefail
cd "$(dirname "$0")/.."
FAIL=0
note() { printf '%s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; FAIL=1; }
ok()   { printf 'ok    %s\n' "$*"; }

# ① compose 校验
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  (cd docker && docker compose config -q) && ok "docker compose config" || bad "docker compose config"
else
  python3 - << 'PY' && ok "compose yaml parse (python fallback)" || { bad "compose yaml parse"; }
import yaml; yaml.safe_load(open('docker/docker-compose.yml'))
PY
fi

# ② 违禁词(排除规则声明所在的元文件与规划目录；含「省略」整词，禁止裸匹配「略」)
# 排除集冻结 D-08: .planning/ PHASES.md CLAUDE.md AGENTS.md scripts/qa_check.sh scripts/README.md docs/README.md
HITS=$(grep -rn --include='*.md' --include='*.java' --include='*.py' \
        -E '(TODO|FIXME|自行实现|请参考官网|省略)' . \
      | grep -v -E '(\./\.planning/|PHASES\.md|CLAUDE\.md|AGENTS\.md|scripts/README\.md|scripts/qa_check\.sh|docs/README\.md)' || true)
if [ -n "$HITS" ]; then bad "违禁词:"; printf '%s\n' "$HITS"; else ok "违禁词扫描"; fi

# ③ Markdown 相对链接存在性(锚点忽略,外链忽略；排除 .planning；必须落在仓库根内，防 ../ 逃逸假绿)
LINKFAIL=0
ROOT="$(pwd -P)"
while IFS=: read -r f link; do
  tgt="${link%%#*}"
  [ -z "$tgt" ] && continue
  case "$tgt" in http*|mailto*) continue ;; esac
  case "$f" in ./.planning/*|.planning/*) continue ;; esac
  cand="$(cd "$(dirname "$f")" && python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$tgt" 2>/dev/null)" \
    || { bad "断链 $f → $link"; LINKFAIL=1; continue; }
  case "$cand" in
    "$ROOT"|"$ROOT"/*) ;;
    *) bad "断链(越界) $f → $link"; LINKFAIL=1; continue ;;
  esac
  if [ ! -e "$cand" ]; then
    bad "断链 $f → $link"; LINKFAIL=1
  fi
done < <(grep -rno --include='*.md' -E '\]\(([^)]+)\)' . \
         --exclude-dir=.planning \
         | sed -E 's/\]\(([^)]+)\)/\1/' | sed 's/:[0-9]*:/:/' )
[ "$LINKFAIL" -eq 0 ] && ok "Markdown 相对链接"

# ④ 案例计数(main 方法数；口径仅 examples/ 下 public static void main，D-01/D-03)
MAINS=$(grep -rl --include='*.java' 'public static void main' examples | wc -l | tr -d ' ')
note "info  可运行作业数(main 计数)= $MAINS"
[ "$MAINS" -ge 100 ] && ok "案例数 ≥ 100" || bad "案例数不足:$MAINS < 100"

# 文档行数仅诊断(不再硬失败;质量以实质内容与违禁词/断链为准)
DOC_LINES=$(find . -name '*.md' -not -path './.planning/*' -not -path './.git/*' \
  -print0 | xargs -0 wc -l | tail -1 | awk '{print $1}')
DOC_LINES="${DOC_LINES:-0}"
note "info  文档行数(excl .planning/.git)= ${DOC_LINES} (diagnostic only, not a hard gate)"

# ⑤ Maven 编译(硬失败：无 mvn 或 compile 失败均 bad，禁止 warn 跳过，D-07)
if ! command -v mvn >/dev/null 2>&1; then
  bad "未检测到 mvn：合入前必须在本机安装 Maven 并通过 examples 编译"
else
  if (cd examples && mvn -q -T1C -DskipTests compile); then
    ok "mvn compile 全模块"
  else
    bad "mvn compile 失败（禁止 warn 跳过；见 D-07）"
  fi
fi

# ENG-01…04 终检（独立脚本硬依赖，D-10；缺失视为门禁失败，禁止 soft-skip 假绿）
if [ -f scripts/eng_audit.sh ]; then
  bash scripts/eng_audit.sh || bad "eng_audit"
else
  bad "scripts/eng_audit.sh 不存在（ENG 终检为硬门禁，禁止跳过）"
fi

if [ "$FAIL" -eq 0 ]; then note "== QA PASS =="; else note "== QA FAIL =="; exit 1; fi
