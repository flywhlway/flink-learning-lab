#!/usr/bin/env bash
# 安装 Argo CD（Helm chart 10.1.4 / app v3.4.5，SSOT 见根 README）并登记 p03 Application。
# PROD-03 / D-07：单一 GitOps=Argo CD；禁止 PyPI 伪 argocd 包。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

ARGO_CHART_VERSION="${ARGO_CHART_VERSION:-10.1.4}"
ARGO_HELM_REPO_NAME="${ARGO_HELM_REPO_NAME:-argo}"
ARGO_HELM_REPO_URL="${ARGO_HELM_REPO_URL:-https://argoproj.github.io/argo-helm}"
ARGO_RELEASE="${ARGO_RELEASE:-argocd}"
ARGO_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APP_NAME="${APP_NAME:-flinklab-p03-bg}"
APP_FILE="${REPO_ROOT}/production/argocd/application-p03.yaml"
MIRROR_DIR="${REPO_ROOT}/.gitops-mirror"
BARE_REPO="${MIRROR_DIR}/flink-learning-lab.git"
MIRROR_NAME="gitops-mirror"
CHART_PATH="production/charts/p03-vehicle-alert"

bash "${SCRIPT_DIR}/check_env.sh"

origin_url() {
  git -C "${REPO_ROOT}" remote get-url origin 2>/dev/null || true
}

to_https_url() {
  local url="$1"
  if [[ "${url}" =~ ^git@github\.com:(.+)$ ]]; then
    echo "https://github.com/${BASH_REMATCH[1]}"
    return
  fi
  if [[ "${url}" =~ ^ssh://git@github\.com/(.+)$ ]]; then
    echo "https://github.com/${BASH_REMATCH[1]}"
    return
  fi
  echo "${url}"
}

remote_has_chart() {
  local url="$1"
  local tmp
  tmp="$(mktemp -d)"
  # 浅探测：ls-remote 通 + 本地是否已 push chart（以 origin/main 树为准）
  if ! git ls-remote --heads "${url}" >/dev/null 2>&1; then
    rm -rf "${tmp}"
    return 1
  fi
  # 尝试从 origin/main 看 path（本地已 fetch 时）
  if git -C "${REPO_ROOT}" rev-parse --verify origin/main >/dev/null 2>&1; then
    if git -C "${REPO_ROOT}" ls-tree -r --name-only origin/main -- "${CHART_PATH}/Chart.yaml" 2>/dev/null | grep -q Chart.yaml; then
      rm -rf "${tmp}"
      return 0
    fi
  fi
  rm -rf "${tmp}"
  return 1
}

refresh_local_mirror() {
  echo "==> 刷新本机 GitOps bare 镜像 ${BARE_REPO}"
  mkdir -p "${MIRROR_DIR}"
  if [[ ! -d "${BARE_REPO}" ]]; then
    git -C "${REPO_ROOT}" clone --bare "${REPO_ROOT}" "${BARE_REPO}"
  else
    # 将当前 HEAD 推入 bare（含本分支全部可达提交）
    git -C "${REPO_ROOT}" push --force "${BARE_REPO}" "HEAD:refs/heads/dev" >/dev/null
    git -C "${REPO_ROOT}" push --force "${BARE_REPO}" "HEAD:refs/heads/main" >/dev/null 2>&1 || true
    git --git-dir="${BARE_REPO}" symbolic-ref HEAD refs/heads/dev >/dev/null 2>&1 || true
  fi
  git --git-dir="${BARE_REPO}" update-server-info
  # dumb HTTP 需要 info/refs 与 objects/info/packs
  echo "ok mirror=${BARE_REPO}"
}

deploy_gitops_mirror() {
  echo "==> 部署集群内 git-daemon 镜像（hostPath → OrbStack；alpine git-daemon）"
  # alpine/git 精简包无 daemon；用 alpine + apk git-daemon 提供 git:// 协议
  kubectl -n "${ARGO_NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: gitops-mirror-repo
  namespace: ${ARGO_NAMESPACE}
  labels:
    argocd.argoproj.io/secret-type: repository
stringData:
  type: git
  name: gitops-mirror
  url: git://${MIRROR_NAME}.${ARGO_NAMESPACE}.svc.cluster.local/flink-learning-lab.git
  insecure: "true"
  enableLfs: "false"
---
apiVersion: v1
kind: Service
metadata:
  name: ${MIRROR_NAME}
  namespace: ${ARGO_NAMESPACE}
  labels:
    app.kubernetes.io/name: ${MIRROR_NAME}
spec:
  selector:
    app.kubernetes.io/name: ${MIRROR_NAME}
  ports:
    - name: git
      port: 9418
      targetPort: 9418
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${MIRROR_NAME}
  namespace: ${ARGO_NAMESPACE}
  labels:
    app.kubernetes.io/name: ${MIRROR_NAME}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${MIRROR_NAME}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${MIRROR_NAME}
    spec:
      containers:
        - name: git-daemon
          image: alpine:3.20
          imagePullPolicy: IfNotPresent
          command:
            - sh
            - -c
            - |
              set -e
              apk add --no-cache git git-daemon >/dev/null
              exec git daemon --verbose --export-all --base-path=/git \
                --listen=0.0.0.0 --port=9418 --reuseaddr /git
          ports:
            - containerPort: 9418
          volumeMounts:
            - name: repos
              mountPath: /git
              readOnly: true
          readinessProbe:
            tcpSocket:
              port: 9418
            initialDelaySeconds: 5
            periodSeconds: 5
      volumes:
        - name: repos
          hostPath:
            path: ${MIRROR_DIR}
            type: Directory
EOF
  kubectl -n "${ARGO_NAMESPACE}" rollout status "deployment/${MIRROR_NAME}" --timeout=180s
}

echo "==> helm repo add/update ${ARGO_HELM_REPO_NAME}"
helm repo add "${ARGO_HELM_REPO_NAME}" "${ARGO_HELM_REPO_URL}" 2>/dev/null \
  || helm repo add "${ARGO_HELM_REPO_NAME}" "${ARGO_HELM_REPO_URL}" --force-update
helm repo update "${ARGO_HELM_REPO_NAME}" >/dev/null

echo "==> helm upgrade --install ${ARGO_RELEASE} (chart ${ARGO_CHART_VERSION})"
helm upgrade --install "${ARGO_RELEASE}" "${ARGO_HELM_REPO_NAME}/argo-cd" \
  --namespace "${ARGO_NAMESPACE}" \
  --create-namespace \
  --version "${ARGO_CHART_VERSION}" \
  --set configs.params."server\.insecure"=true \
  --set server.service.type=ClusterIP \
  --wait \
  --timeout 10m

echo "==> 等待 argocd-server Available"
kubectl -n "${ARGO_NAMESPACE}" wait --for=condition=Available deployment \
  -l app.kubernetes.io/name=argocd-server \
  --timeout=180s

if ! kubectl get crd applications.argoproj.io >/dev/null 2>&1; then
  echo "FAIL: CRD applications.argoproj.io 不存在" >&2
  exit 1
fi

ORIGIN_RAW="$(origin_url)"
ORIGIN_HTTPS="$(to_https_url "${ORIGIN_RAW}")"
REPO_URL=""
TARGET_REVISION="HEAD"
SOURCE_MODE=""

if [[ -n "${ORIGIN_HTTPS}" ]] && remote_has_chart "${ORIGIN_HTTPS}"; then
  REPO_URL="${ORIGIN_HTTPS}"
  SOURCE_MODE="origin"
  echo "==> 使用 origin 作为 Argo 源: ${REPO_URL}"
else
  echo "==> origin 不可达或不含 ${CHART_PATH} — 启用本机 git HTTP 镜像"
  refresh_local_mirror
  deploy_gitops_mirror
  REPO_URL="git://${MIRROR_NAME}.${ARGO_NAMESPACE}.svc.cluster.local/flink-learning-lab.git"
  TARGET_REVISION="dev"
  SOURCE_MODE="local-mirror"
fi

# 探测当前 BG 镜像 tag，注入 helm 参数避免无谓切换
LIVE_TAG="dev-green"
if LIVE_IMAGE="$(kubectl -n flink get flinkbluegreendeployment p03-vehicle-alert-bg \
  -o jsonpath='{.spec.template.spec.image}' 2>/dev/null || true)"; then
  if [[ -n "${LIVE_IMAGE}" && "${LIVE_IMAGE}" == *:* ]]; then
    LIVE_TAG="${LIVE_IMAGE##*:}"
  fi
fi

TMP_APP="$(mktemp)"
# 改写 repoURL / targetRevision / image.tag
python3 - "${APP_FILE}" "${TMP_APP}" "${REPO_URL}" "${TARGET_REVISION}" "${LIVE_TAG}" <<'PY'
import sys, re
src, dst, repo, rev, tag = sys.argv[1:6]
text = open(src, encoding="utf-8").read()
text = re.sub(r"(?m)^(\s*repoURL:\s*).*$", rf"\1{repo}", text, count=1)
text = re.sub(r"(?m)^(\s*targetRevision:\s*).*$", rf"\1{rev}", text, count=1)
text = re.sub(
    r"(?m)^(\s*-\s*name:\s*image\.tag\s*\n\s*value:\s*)\".*\"",
    rf'\1"{tag}"',
    text,
    count=1,
)
open(dst, "w", encoding="utf-8").write(text)
PY

echo "==> kubectl apply Application ${APP_NAME} (source=${SOURCE_MODE} tag=${LIVE_TAG})"
kubectl apply -f "${TMP_APP}"
rm -f "${TMP_APP}"

# 不在 sync 前 helm uninstall：uninstall 会删掉运行中的 FlinkBlueGreenDeployment。
# 若仍有 Helm release，仅在 sync 成功后解除登记（资源已由 Argo 接管）。

echo "==> 触发显式 sync"
kubectl -n "${ARGO_NAMESPACE}" patch application "${APP_NAME}" --type merge \
  -p "{\"operation\":{\"initiatedBy\":{\"username\":\"install-argocd\"},\"sync\":{\"revision\":\"${TARGET_REVISION}\",\"syncStrategy\":{\"apply\":{\"force\":false}}}}}" \
  >/dev/null

echo "==> 等待 sync=Synced（最长 180s）"
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  sync_status="$(kubectl -n "${ARGO_NAMESPACE}" get application "${APP_NAME}" \
    -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
  health="$(kubectl -n "${ARGO_NAMESPACE}" get application "${APP_NAME}" \
    -o jsonpath='{.status.health.status}' 2>/dev/null || true)"
  echo "    sync=${sync_status:-?} health=${health:-?}"
  if [[ "${sync_status}" == "Synced" && "${health}" =~ ^(Healthy|Progressing)$ ]]; then
    break
  fi
  # Progressing 在 Flink CR 调谐时常见；Synced+Healthy 为终态偏好
  if [[ "${sync_status}" == "Synced" && "${health}" == "Healthy" ]]; then
    break
  fi
  sleep 5
done

sync_status="$(kubectl -n "${ARGO_NAMESPACE}" get application "${APP_NAME}" \
  -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
health="$(kubectl -n "${ARGO_NAMESPACE}" get application "${APP_NAME}" \
  -o jsonpath='{.status.health.status}' 2>/dev/null || true)"

if [[ "${sync_status}" != "Synced" ]]; then
  echo "FAIL: Application sync.status=${sync_status:-<empty>} health=${health:-<empty>}" >&2
  kubectl -n "${ARGO_NAMESPACE}" get application "${APP_NAME}" -o yaml | tail -80 >&2 || true
  exit 1
fi

# sync 成功后再卸 Helm release 登记（避免与 Argo 双管家；CR 已由 Argo 管理）
if helm status p03-vehicle-alert -n flink >/dev/null 2>&1; then
  echo "==> sync 已绿：解除 Helm release 登记 p03-vehicle-alert（CR 保留给 Argo）"
  # helm uninstall 会删资源——此处仅在确认 Argo 已管理后执行；
  # 为避免误删，改用标注移交：不 uninstall，只打印提示。
  echo "    提示: 若需去掉 Helm 元数据，可 kubectl annotate/label 清理 managed-by；勿盲目 uninstall"
fi

echo
echo "---- Argo CD initial admin 密码（仅本机；禁止写入仓库）----"
echo "kubectl -n ${ARGO_NAMESPACE} get secret argocd-initial-admin-secret \\"
echo "  -o jsonpath='{.data.password}' | base64 -d; echo"
echo "UI: kubectl -n ${ARGO_NAMESPACE} port-forward svc/${ARGO_RELEASE}-server 8080:443"
echo "------------------------------------------------------------"
echo "ok argocd chart=${ARGO_CHART_VERSION} app=${APP_NAME} sync=${sync_status} health=${health:-unknown} source=${SOURCE_MODE} repoURL=${REPO_URL}"
