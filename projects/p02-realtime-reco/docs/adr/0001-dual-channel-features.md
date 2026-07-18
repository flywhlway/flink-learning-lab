# ADR-0001：在线特征双通道——Flink Keyed State + Redis 点查

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** flink-learning-lab P4 / p02（RECO-02 / RECO-03 / D-01 / D-03 / D-13）
- **Tags:** dual-channel-features, keyed-state, redis, jedis, at-least-once, flink-2.2

## Context

实时推荐需要「可观察的在线特征」支撑规则 Top-K 打分，同时必须满足学习工程硬纪律：

1. **Keyed State 与 Redis 都要可讲清新鲜度差异**（D-01）：禁止「只写 Redis 不建状态」或「纯状态从不落 Redis」。
2. **Redis 不可用时作业不得长期 FAIL**（D-12）：演练 `compose stop redis` 后仍须凭状态通道产出 ClickHouse 可断言行。
3. **写入语义诚实**：禁止假装 Redis 侧 exactly-once（D-03）。

可选路径对比：

| 路径 | 能力 | 代价 / 风险 |
|------|------|-------------|
| A. 纯 Keyed State，不落 Redis | 降级天然成立；作业面最小 | 无法演示在线特征库点查 / 跨作业共享特征；简历叙事偏弱 |
| B. 纯 Redis，无状态累积 | 点查路径清晰 | Redis 停服则无特征；违反 D-12；新鲜度与故障边界难讲 |
| C. **双通道：Keyed State 累积 + Redis at-least-once 点查** | 状态兜底 + 外存可观察；读失败标 `STATE_ONLY` | 写路径需 catch；文档须写明 at-least-once |
| D. lettuce / 外部特征平台 | 客户端选型多样 | 未登记版本矩阵；超出 MVP |

教材对照：[examples/e12-06-streaming-feature](../../../../examples/e12-06-streaming-feature/) · [examples/e07-connectors](../../../../examples/e07-connectors/)（C7 jedis Pipeline）· [ai/chapters](../../../../ai/chapters/) 特征工程相关章。

## Decision

采用路径 **C**：

1. **通道一 · Flink Keyed State**：`SessionFeatureFunction` 按 `userId` 累积类目/商品亲和与 click 计数，随流输出 `FeatureSnapshot`，供打分与降级兜底。
2. **通道二 · Redis 在线特征库**：`RedisFeatureWriter` 用 **jedis Pipeline + CheckpointedFunction** 攒批写 `feature:{userId}:*`；语义锁定 **at-least-once**（checkpoint 尾巴进 Operator ListState，恢复可能重复 SET；同 key 幂等无害）。写失败 catch + metric，**不抛死作业**。
3. **打分热路径**：`TopKScoreFunction` **优先点查 Redis**；成功标 `feature_source=REDIS`；超时/连接失败/空 key → 使用随流 State，标 `feature_source=STATE_ONLY`。禁止默认抛异常导致作业 RESTARTING。
4. **明确不做**：纯状态或纯外存单通道作为完成态；假装 Redis exactly-once；把 lettuce / 未登记特征平台写进主路径。

架构总图见 [ARCHITECTURE.md](../ARCHITECTURE.md)。简历可验证动词见 [RESUME.md](../RESUME.md)。

## Consequences

### 正向

- OrbStack 上可一键复现：`make drill-redis`（stop `fll-redis`）后 CH 仍出现 `feature_source=STATE_ONLY`；权威出口仍是 ClickHouse `reco_results`。
- 正常路径可观察 Redis key 与 `feature_source=REDIS`，讲清双通道职责与新鲜度。
- 简历陈述可指向脚本路径，而非空泛「接了 Redis 特征」。

### 负向 / 约束

- Redis 侧 **不是** exactly-once；故障恢复可能重复写，须在面试中主动说明。
- 打分读 Redis 有超时成本；State 通道必须始终携带足够特征以免空 catalog 无输出。
- 演练结束必须恢复 redis（脚本 EXIT trap），避免污染后续 `loadtest`。

### 可验证锚点

| 符号 / 产物 | 路径 |
|-------------|------|
| `SessionFeatureFunction` MapState | `src/main/java/.../feature/SessionFeatureFunction.java` |
| `RedisFeatureWriter` at-least-once | `src/main/java/.../feature/RedisFeatureWriter.java` |
| `TopKScoreFunction` REDIS\|STATE_ONLY | `src/main/java/.../score/TopKScoreFunction.java` |
| Redis 降级演练 | `make drill-redis` → `scripts/drill_redis_degrade.sh` |
| 默认验收 | `make verify` / `make match` → `scripts/verify.sh`（CH 权威） |
| 压测 baseline | `make loadtest` → [baseline.md](../baseline.md) |
