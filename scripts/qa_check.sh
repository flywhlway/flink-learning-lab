#!/usr/bin/env bash
# 仓库 QA 自检(PHASES.md 约定的验收工具)。用法:bash scripts/qa_check.sh
# 六硬门:① compose YAML 可解析 ② 违禁词扫描 ③ Markdown 相对链接存在性
#         ④ 可运行案例计数 ≥100 ⑤ 文档行数 ≥30000 ⑥ Maven 全模块编译(硬失败)
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

# ③ Markdown 相对链接存在性(锚点忽略,外链忽略；排除 .planning 规划稿；仅检查路径存在)
LINKFAIL=0
while IFS=: read -r f link; do
  tgt="${link%%#*}"
  [ -z "$tgt" ] && continue
  case "$tgt" in http*|mailto*) continue ;; esac
  case "$f" in ./.planning/*|.planning/*) continue ;; esac
  if [ ! -e "$(dirname "$f")/$tgt" ]; then
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

# ⑤ 文档行数(全部 *.md，排除 .planning/ 与 .git/，D-04/D-06)
DOC_LINES=$(find . -name '*.md' -not -path './.planning/*' -not -path './.git/*' \
  -print0 | xargs -0 wc -l | tail -1 | awk '{print $1}')
note "info  文档行数(excl .planning/.git)= $DOC_LINES"
[ "$DOC_LINES" -ge 30000 ] && ok "文档行数 ≥ 30000" || bad "文档行数不足:$DOC_LINES < 30000"

# ⑥ Maven 编译(硬失败：无 mvn 或 compile 失败均 bad，禁止 warn 跳过，D-07)
if ! command -v mvn >/dev/null 2>&1; then
  bad "未检测到 mvn：合入前必须在本机安装 Maven 并通过 examples 编译"
else
  if (cd examples && mvn -q -T1C -DskipTests compile); then
    ok "mvn compile 全模块"
  else
    bad "mvn compile 失败（禁止 warn 跳过；见 D-07）"
  fi
fi

# ENG-01…04 终检（独立脚本；存在则调用，D-10）
if [ -f scripts/eng_audit.sh ]; then
  bash scripts/eng_audit.sh || bad "eng_audit"
else
  note "warn  scripts/eng_audit.sh 不存在，跳过 ENG 终检"
fi

if [ "$FAIL" -eq 0 ]; then note "== QA PASS =="; else note "== QA FAIL =="; exit 1; fi
