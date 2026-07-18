#!/usr/bin/env bash
# K8s → compose 中间件连通性探针（PROD-02 / RESEARCH Pattern 3）。
# 锁定 bootstrap：host.docker.internal:9095（compose Kafka K8S listener）。
# 说明：host.docker.internal:9094（EXTERNAL）协议失败，因 advertised 为 localhost:9094。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-host.docker.internal:9095}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.1}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "FAIL: kubectl 未安装" >&2
  exit 1
fi

echo "==> TCP 探针（普通 Pod → host.docker.internal）"
kubectl delete pod flinklab-tcp-probe --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl run flinklab-tcp-probe --restart=Never --image=busybox:1.36 --command -- \
  sh -c 'nc -zvw5 host.docker.internal 9095 && nc -zvw5 host.docker.internal 8123 && nc -zvw5 host.docker.internal 9000 && echo TCP_OK'
for i in $(seq 1 30); do
  PHASE="$(kubectl get pod flinklab-tcp-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo Missing)"
  [[ "${PHASE}" == "Succeeded" || "${PHASE}" == "Failed" ]] && break
  sleep 1
done
TCP_LOG="$(kubectl logs flinklab-tcp-probe 2>/dev/null || true)"
TCP_PHASE="$(kubectl get pod flinklab-tcp-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo Missing)"
kubectl delete pod flinklab-tcp-probe --ignore-not-found --wait=true >/dev/null 2>&1 || true
if [[ "${TCP_PHASE}" != "Succeeded" ]] || ! echo "${TCP_LOG}" | grep -q TCP_OK; then
  echo "FAIL: host.docker.internal TCP 不可达（compose 是否已发布 9095/8123/9000？）phase=${TCP_PHASE}" >&2
  echo "${TCP_LOG}" >&2
  exit 1
fi
echo "ok tcp host.docker.internal:{9095,8123,9000}"

echo "==> Kafka 协议探针（普通 Pod + ${BOOTSTRAP}）"
kubectl delete pod flinklab-kafka-probe --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl run flinklab-kafka-probe --restart=Never \
  --image="${KAFKA_IMAGE}" --command -- \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP}"
for i in $(seq 1 60); do
  PHASE="$(kubectl get pod flinklab-kafka-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo Missing)"
  if [[ "${PHASE}" == "Succeeded" ]]; then
    break
  fi
  if [[ "${PHASE}" == "Failed" ]]; then
    echo "FAIL: Kafka 协议探针 Failed（bootstrap=${BOOTSTRAP}）" >&2
    kubectl logs flinklab-kafka-probe 2>&1 | tail -40 >&2 || true
    kubectl delete pod flinklab-kafka-probe --ignore-not-found --wait=true >/dev/null 2>&1 || true
    exit 1
  fi
  sleep 2
done
PHASE="$(kubectl get pod flinklab-kafka-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo Missing)"
if [[ "${PHASE}" != "Succeeded" ]]; then
  echo "FAIL: Kafka 协议探针超时 phase=${PHASE}" >&2
  kubectl logs flinklab-kafka-probe 2>&1 | tail -40 >&2 || true
  kubectl delete pod flinklab-kafka-probe --ignore-not-found --wait=true >/dev/null 2>&1 || true
  exit 1
fi
kubectl delete pod flinklab-kafka-probe --ignore-not-found --wait=true >/dev/null 2>&1 || true

echo "LOCKED_BOOTSTRAP=${BOOTSTRAP}"
echo "LOCKED_TOPOLOGY=host.docker.internal:9095 (compose KAFKA listener K8S)"
echo "ok probe_kafka_from_k8s"
