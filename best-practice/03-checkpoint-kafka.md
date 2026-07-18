# 03 · checkpoint / Kafka 事务 / 端到端一致性

## 规则

1. **checkpoint 间隔显著小于 Kafka 事务超时**；同时抬高 broker `transaction.max.timeout.ms` 放行（总则 #2）。
2. **端到端 EXACTLY_ONCE =** 可重置源 + Flink checkpoint + 事务/幂等 sink；缺一不可。checkpoint 成功 ≠ 下游已 EOS。
3. **远程 CP 存储**（本仓库 MinIO）权限与磁盘监控必做；失败看 `numberOfFailedCheckpoints`。
4. **大状态开增量 checkpoint**（RocksDB）（总则 #9）。

## 理由

事务超时短于对齐+上传时间 → 事务被 abort → 看似 EOS 实则丢数或重复。值班必须以指标证伪。

## 反例

- checkpoint 10s、事务超时 5s，「正好一次」作业周期性丢窗口结果。
- 只用 at-least-once Kafka sink 却对业务承诺 EOS。

## 落地互链

- 连接器语义教材：[`docs/07-connectors/`](../docs/07-connectors/)
- 容错教材：[`docs/04-checkpoint/`](../docs/04-checkpoint/)
- 看板：[`monitoring/job-deepdive.json`](../monitoring/job-deepdive.json)
