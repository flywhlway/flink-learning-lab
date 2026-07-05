#!/usr/bin/env bash
# 下载 Paimon/Iceberg 相关 jar 到 docker/jobs/,供 SQL Client 内 `ADD JAR` 加载。
# 用法:bash scripts/fetch_lakehouse_jars.sh
# 网络受限环境:改为从公司内部镜像/Maven仓库同源下载,文件名与版本保持一致即可。
set -euo pipefail
cd "$(dirname "$0")/.."
DEST=docker/jobs
mkdir -p "$DEST"

FLINK_VERSION="2.2.1"
PAIMON_VERSION="1.0.1"
ICEBERG_VERSION="1.7.1"

declare -A JARS=(
  ["paimon-flink-${FLINK_VERSION%.*}.jar"]="https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-${FLINK_VERSION%.*}/${PAIMON_VERSION}/paimon-flink-${FLINK_VERSION%.*}-${PAIMON_VERSION}.jar"
  ["paimon-s3-${PAIMON_VERSION}.jar"]="https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar"
  ["iceberg-flink-runtime-${FLINK_VERSION%.*}-${ICEBERG_VERSION}.jar"]="https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION%.*}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION%.*}-${ICEBERG_VERSION}.jar"
)

for name in "${!JARS[@]}"; do
  url="${JARS[$name]}"
  if [ -f "$DEST/$name" ]; then
    echo "skip  已存在 $DEST/$name"
    continue
  fi
  echo "fetch $name  ← $url"
  if curl -fsSL -o "$DEST/$name" "$url"; then
    echo "ok    $name"
  else
    echo "warn  下载失败(离线环境常见):$name — 请手动放置到 $DEST/ 或改用内部镜像源"
    rm -f "$DEST/$name"
  fi
done

echo "== 完成。SQL Client 中用 ADD JAR '/opt/flink/usrlib/<文件名>' 加载 =="
