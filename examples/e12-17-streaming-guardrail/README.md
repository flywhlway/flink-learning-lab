# e12-17 · Streaming Guardrail:流式内容护栏

> 对应 [ai/chapters/17-streaming-guardrail.md](../../ai/chapters/17-streaming-guardrail.md) · Level:L4
> 运行:`mvn -q -Plocal compile exec:java -pl e12-17-streaming-guardrail -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingGuardrailJob`

## 背景

完整复用 e03-C7 Broadcast State 骨架,把"车辆信号阈值"换成"LLM 输出内容审查"——证明同一套动态规则机制可以跨领域直接复用。用随机文本模拟 LLM 输出,避免依赖外部模型服务。

## 验证方式

观察 `RULE-UPDATED` 日志出现的时间点,对照其后 `BLOCK` 行是否命中新规则的关键词(`泄露内部信息`)——命中即证明热更新生效,无需重启作业。

## 源码要点

- 与 e03-C7 唯一的实质差异是业务领域(信号阈值 → 关键词黑名单),底层 Broadcast State 机制完全相同。
- 生产版本应叠加第 17 章讲的三层护栏设计(输入/输出/行为),本 Demo 只演示输出护栏这一层。

## 面试题

见 ai/chapters/17-streaming-guardrail.md 第 8 节。
