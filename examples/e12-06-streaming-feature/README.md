# e12-06 · Streaming Feature:实时特征工程 + Redis 特征库

> 对应 [ai/chapters/06-streaming-feature.md](../../ai/chapters/06-streaming-feature.md) · Level:L3
> 前置:`cd docker && make up`(需要 redis 服务)
> 运行:`mvn -q -Plocal compile exec:java -pl e12-06-streaming-feature -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingFeatureJob`

## 背景

案例二(实时推荐系统)的核心技术铺垫:窗口特征(近期行为统计)+ 状态特征(会话内累积)双通道产出,写入 Redis 供在线推理低延迟点查。三个组成部分(滑动窗口、MapState、jedis Pipeline)均为此前模块已验证的模式组合,零新增风险。

## 验证方式

`redis-cli KEYS 'feature:*'` 可见两类特征持续写入;`FLUSH N features → redis` 输出表明攒批生效。

## 源码要点

- 本 Demo 为教学简化版:省略了 e07-C7 完整的 Operator State 容错(生产版本应叠加,详见 e07-C7 源码)。
- 窗口通道与状态通道是两种不同"新鲜度"的特征来源,ai/06 第 3 节详解两者的选择依据。

## 面试题

见 ai/chapters/06-streaming-feature.md 第 8 节。
