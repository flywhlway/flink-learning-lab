# e12-22 · Streaming Prompt:Prompt 版本化与确定性灰度

> 对应 [ai/chapters/22-streaming-prompt.md](../../ai/chapters/22-streaming-prompt.md) · Level:L4
> 运行:`mvn -q -Plocal compile exec:java -pl e12-22-streaming-prompt -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingPromptGrayReleaseJob`

## 背景

与 e12-17 同源的 Broadcast State 模式,应用在"Prompt 版本灰度"场景。核心验证点是**确定性分流**:用请求所属实体的哈希值而非随机数决定灰度分支,保证同一实体全程看到一致的行为。

## 验证方式

观察输出中同一 `entity=` 的多行记录,`version` 字段应保持不变(要么一直 stable,要么一直 canary),不会出现同一实体在 stable/canary 间跳变。`PROMPT-UPDATED` 日志出现后,约 30% 的实体应切换到 canary 版本(且是同一批实体,不是随机换批)。

## 源码要点

- `Math.floorMod(entityId.hashCode(), 100) < ratio*100` 是确定性分流的核心一行代码——足够简单,但很多生产系统栽在"忘记用确定性哈希、直接用随机数分流"这个细节上。
- 与 e12-17 对比阅读:两者是同一个 Broadcast State 机制在"内容审查"与"版本灰度"两个不同业务场景的应用,建议对照学习加深"机制可复用、场景可迁移"的体会。

## 面试题

见 ai/chapters/22-streaming-prompt.md 第 8 节。

---

# e12-22-streaming-prompt · 八段式扩写（Wave 2）

## 1. 背景

本模块演示「流式提示」。目标是在零依赖或受控依赖下跑通机制，而不是堆模型。对应教材章节：`../../ai/chapters/`（ai/22）。生产降级对照 p01。

## 2. 架构

```mermaid
flowchart LR
  In[事件源 datagen/Kafka] --> Op[本模块算子]
  Op --> Out[主流输出]
  Op --> Side[旁路/降级]
```

算子链保持可观测：主流契约稳定，超时/拒识/超预算走旁路。主类焦点：Prompt + Hash。

## 3. 代码锚点

阅读 `src/main/java/**/*.java` 中带 `public static void main` 的作业；注意 `.uid(...)` 与旁路 OutputTag。模块坐标：`examples/e12-22-streaming-prompt`。

## 4. 启动

```bash
(cd docker && docker compose up -d)  # 若需要基座
(cd examples && mvn -pl e12-22-streaming-prompt -am -DskipTests package)
# 提交主类见下方表格；OrbStack arm64 实测
```

## 5. 验证

- UI RUNNING
- 主流有输出；注入故障后旁路有信号
- `mvn -pl e12-22-streaming-prompt -am -DskipTests compile` 通过
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

围绕「流式提示」第 1 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 2

围绕「流式提示」第 2 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 3

围绕「流式提示」第 3 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 4

围绕「流式提示」第 4 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 深潜 5

围绕「流式提示」第 5 个决策点：延迟预算、成本、正确性、降级、可观测。写出若相反选择会发生什么，并指出本模块哪个类可演示。

## 与生产项目对照

- p01：`../../projects/p01-log-ai-platform/README.md`（AI off 默认可跑）
- p02：特征/召回对照（若主题相关）
- 规范：`../../best-practice/08-ai-degrade.md`

## 验证记录模板

日期 / 环境 OrbStack / 命令 / 期望 / 实际 / 日志路径。通过后才可在笔记中勾选本模块。

