# e12-06 · Streaming Feature:实时特征工程 + Redis 特征库

> 对应 [ai/chapters/06-streaming-feature.md](../../ai/chapters/06-streaming-feature.md) · Level:L3
> 前置:`cd docker && make up`(需要 redis 服务)
> 运行:`mvn -q -Plocal compile exec:java -pl e12-06-streaming-feature -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingFeatureJob`

## 背景

案例二(实时推荐系统)的核心技术铺垫:窗口特征(近期行为统计)+ 状态特征(会话内累积)双通道产出,写入 Redis 供在线推理低延迟点查。三个组成部分(滑动窗口、MapState、jedis Pipeline)均为此前模块已验证的模式组合,零新增风险。

## 验证方式

`redis-cli KEYS 'feature:*'` 可见两类特征持续写入;`FLUSH N features → redis` 输出表明攒批生效。

## 源码要点

- 本 Demo 为教学简化版:未包含 e07-C7 完整的 Operator State 容错(生产版本应叠加,详见 e07-C7 源码)。
- 窗口通道与状态通道是两种不同"新鲜度"的特征来源,ai/06 第 3 节详解两者的选择依据。

## 面试题

见 ai/chapters/06-streaming-feature.md 第 8 节。

---

# e12-06-streaming-feature · 八段式扩写（Wave 2）

## 1. 背景

本模块演示「流式特征」。目标是在零依赖或受控依赖下跑通机制，而不是堆模型。对应教材章节：`../../ai/chapters/`（ai/06）。生产降级对照 p01。

## 2. 架构

```mermaid
flowchart LR
  In[事件源 datagen/Kafka] --> Op[本模块算子]
  Op --> Out[主流输出]
  Op --> Side[旁路/降级]
```

算子链保持可观测：主流契约稳定，超时/拒识/超预算走旁路。主类焦点：StreamingFeature + Delta。

## 3. 代码锚点

阅读 `src/main/java/**/*.java` 中带 `public static void main` 的作业；注意 `.uid(...)` 与旁路 OutputTag。模块坐标：`examples/e12-06-streaming-feature`。

## 4. 启动

```bash
(cd docker && docker compose up -d)  # 若需要基座
(cd examples && mvn -pl e12-06-streaming-feature -am -DskipTests package)
# 提交主类见下方表格；OrbStack arm64 实测
```

## 5. 验证

- UI RUNNING
- 主流有输出；注入故障后旁路有信号
- `mvn -pl e12-06-streaming-feature -am -DskipTests compile` 通过
- 不引入违禁词

## 6. 踩坑

| 症状 | 根因 | 处置 |
|---|---|---|
| 作业起不来 | 类路径/主类 | 核对 pom 与 -c |
| 无输出 | 源无数据/过滤过严 | 查 datagen 与旁路 |
| 外呼拖死 | 同步阻塞 | 改 Async / 降级 |
| 成本飙升 | 无预算门控 | 软顶+降采样 |

## 7. 最佳实践

- 有状态算子固定 uid；见 `../../best-practice/02-uid-savepoint.md`
- AI/外呼路径必须可降级；见 `../../best-practice/08-ai-degrade.md`
- 反压按三步法；见 `../../best-practice/05-backpressure.md`
- 交叉教材：`../../docs/` 与 `../../ai/chapters/`

## 8. 面试题

对应 `../../interview/L8.md`（AI）或模块相关 Level；用 90 秒讲清定义→机制→反例→仓库路径。


## 深潜 1

围绕「流式特征」第 1 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 2

围绕「流式特征」第 2 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 3

围绕「流式特征」第 3 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 4

围绕「流式特征」第 4 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 5

围绕「流式特征」第 5 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 与生产项目对照

- p01：`../../projects/p01-log-ai-platform/README.md`（AI off 默认可跑）
- p02：特征/召回对照（若主题相关）
- 规范：`../../best-practice/08-ai-degrade.md`

## 验证记录模板

日期 / 环境 OrbStack / 命令 / 期望 / 实际 / 日志路径。通过后才可在笔记中勾选本模块。

