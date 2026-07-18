#!/usr/bin/env bash
# P5 环境门禁：Helm CLI + OrbStack K8s Ready（禁止 PyPI 伪 helm）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v helm >/dev/null 2>&1; then
  echo "FAIL: helm 未安装；尝试 brew install helm（官方 Homebrew formula，禁止 pip install helm）" >&2
  if command -v brew >/dev/null 2>&1; then
    if ! brew install helm; then
      echo "FAIL: brew install helm 失败；请在宿主机手动安装 Helm 3.x/4.x 并确保 command -v helm 成功" >&2
      exit 1
    fi
  else
    echo "FAIL: 无 brew；请手动安装 Helm 3.x/4.x 后重试" >&2
    exit 1
  fi
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "FAIL: kubectl 未安装（OrbStack 通常自带；请启用 K8s 后重试）" >&2
  exit 1
fi

# 优先切到 OrbStack context；若当前已是可用 context 且节点 Ready 亦可
if kubectl config get-contexts orbstack >/dev/null 2>&1; then
  kubectl config use-context orbstack >/dev/null
elif [[ "$(kubectl config current-context 2>/dev/null || true)" == "orbstack" ]]; then
  :
else
  CTX="$(kubectl config current-context 2>/dev/null || true)"
  if [[ -z "$CTX" ]]; then
    echo "FAIL: 无 kubectl context；请先 orb start k8s 并 kubectl config use-context orbstack" >&2
    exit 1
  fi
  echo "WARN: 使用当前 context=${CTX}（文档默认 orbstack）" >&2
fi

NODES_OUT="$(kubectl get nodes 2>&1)" || {
  echo "FAIL: kubectl get nodes 失败（OrbStack K8s 是否已启动？）" >&2
  echo "$NODES_OUT" >&2
  exit 1
}

if ! echo "$NODES_OUT" | awk 'NR>1 && $2=="Ready" { found=1 } END { exit !found }'; then
  echo "FAIL: 无 Ready 节点" >&2
  echo "$NODES_OUT" >&2
  exit 1
fi

HELM_VER="$(helm version --short 2>/dev/null || helm version -c 2>/dev/null | head -1)"
echo "ok helm=${HELM_VER} k8s=Ready"
