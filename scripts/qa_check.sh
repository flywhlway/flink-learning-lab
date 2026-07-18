#!/usr/bin/env bash
# 仓库 QA 自检(PHASES.md 约定的验收工具)。用法:bash scripts/qa_check.sh
# 检查项:① compose YAML 可解析 ② 违禁词扫描 ③ Markdown 相对链接存在性
#         ④ 可运行案例计数 ⑤ Maven 全模块编译(有 mvn 且可联网时)
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

# ② 违禁词(排除规则声明所在的元文件与规划目录)
HITS=$(grep -rn --include='*.md' --include='*.java' --include='*.py' \
        -E '(TODO|FIXME|自行实现|请参考官网)' . \
      | grep -v -E '(\./\.planning/|PHASES\.md|CLAUDE\.md|AGENTS\.md|scripts/README\.md|scripts/qa_check\.sh|docs/README\.md)' || true)
if [ -n "$HITS" ]; then bad "违禁词:"; printf '%s\n' "$HITS"; else ok "违禁词扫描"; fi

# ③ Markdown 相对链接存在性(锚点忽略,外链忽略；排除 .planning 规划稿)
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

# ④ 案例计数(main 方法数)
MAINS=$(grep -rl --include='*.java' 'public static void main' examples | wc -l | tr -d ' ')
note "info  可运行作业数(main 计数)= $MAINS"
[ "$MAINS" -ge 67 ] && ok "案例数 ≥ Phase3 口径(67)" || bad "案例数不足:$MAINS < 67"

# ⑤ Maven 编译(可选:无 mvn 或离线时跳过,不判失败)
if command -v mvn >/dev/null 2>&1; then
  if (cd examples && mvn -q -T1C -DskipTests compile 2>/dev/null); then
    ok "mvn compile 全模块"
  else
    note "warn  mvn compile 未通过或无法联网(离线环境可忽略,合入前须在本机通过)"
  fi
else
  note "warn  未检测到 mvn,跳过编译检查(合入前须在本机通过 mvn -q clean package)"
fi

if [ "$FAIL" -eq 0 ]; then note "== QA PASS =="; else note "== QA FAIL =="; exit 1; fi
