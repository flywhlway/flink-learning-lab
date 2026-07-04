#!/usr/bin/env bash
# 初始化脚本(幂等):Kafka topics + ClickHouse 校验 + 汇总输出
set -euo pipefail
cd "$(dirname "$0")"

echo "==> 等待 Kafka 就绪……"
for i in $(seq 1 30); do
  if docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server localhost:9092 >/dev/null 2>&1; then break; fi
  sleep 2
done

create_topic() {
  local name=$1 partitions=$2
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 >/dev/null
  echo "    topic 就绪: $name (partitions=$partitions)"
}

echo "==> 创建 Kafka topics"
create_topic clicks 4          # e01 输入:点击流
create_topic clicks.agg 4      # e01 输出:窗口聚合
create_topic events.raw 4      # 通用原始事件(后续模块共用)
create_topic events.enriched 4 # 通用加工后事件

echo "==> 校验 ClickHouse"
docker compose exec clickhouse clickhouse-client \
  --user "${CH_USER:-flinklab}" --password "${CH_PASSWORD:-flinklab123}" \
  --query "SELECT 'clickhouse ok', version()" || echo "    (ClickHouse 尚未就绪,可稍后重试 make init)"

echo "==> 校验 Flink REST"
curl -sf http://localhost:8081/overview >/dev/null \
  && echo "    flink ok" || echo "    (Flink 尚未就绪,可稍后重试)"

echo
echo "全部初始化完成。运行 make urls 查看各控制台地址。"
