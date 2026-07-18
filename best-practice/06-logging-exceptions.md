# 06 · 日志与异常

## 规则

1. **脏数据 / 解析失败 → Side Output → 死信 topic 或表**，禁止空 `catch`（总则 #6）。
2. **日志结构化：** 至少含 `job_name`、关键业务键（脱敏后）、算子名；禁止打印密钥与完整 Token。
3. **异常分级：** 可重试（IO 抖动）vs 毒丸（坏 schema）；毒丸旁路，可重试有界。
4. **指标与日志互补：** 计数器证明「发生过」，日志证明「是哪条」。

## 理由

静默丢数使 MTTR 从分钟变天；密钥进日志造成合规事故。

## 反例

- `catch (Exception e) { return; }` 吃掉 JSON 解析错误，CH 条数对不上 Kafka。
- 把 Ollama API 完整 prompt（含用户 PII）打到 INFO。

## 落地互链

- p01 Guardrail / 脱敏：[`projects/p01-log-ai-platform/`](../projects/p01-log-ai-platform/)
- 可观测：[`monitoring/`](../monitoring/)
