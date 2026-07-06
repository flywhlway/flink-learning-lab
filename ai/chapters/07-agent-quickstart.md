# 第 07 章 · Streaming Agent 第一课:Flink Agents 架构与 Java 快速上手

> Demo:e12-07(Java,Flink Agents 0.3 Preview API)· Level:L5
> ⚠️ Preview API 风险:本章代码依据官方 0.3 发布说明与 API 讨论整理,字段/方法签名可能随后续版本调整,以官方 nightly 文档为准。

## 1. 定位:Agent 是流拓扑里的一等公民

Flink Agents 把"一个 Agent"编译成 Flink 拓扑里的一个(或一组)算子——继承 Flink 的分布式执行、状态管理、checkpoint 容错,而不是在 Flink 之外另起一个 Agent 运行时。这是它与"LangGraph + 外部编排"路线的根本区别:**状态与计算在同一个引擎里,不需要额外的一致性桥接**。

## 2. 核心概念与执行模型

```mermaid
flowchart TB
    subgraph DEF[定义层]
        AG[Agent 子类] --> ACT["@Action 注解方法\n每个方法监听特定 Event 类型"]
    end
    subgraph PLAN[编译层]
        ACT --> AP[AgentPlan\n反射提取 Action 元数据+事件路由表]
    end
    subgraph EXEC[执行层]
        AP -->|本地测试| LR[LocalRunner\n单进程内存态]
        AP -->|生产部署| AEO[ActionExecutionOperator\nFlink 分布式算子+容错状态]
    end
```

- **Agent**:业务逻辑的容器,继承 `org.apache.flink.agents.api.Agent`。
- **Action**:用 `@Action(listenEvents = {...})` 标注的方法,声明"监听哪些事件类型";一个 Agent 可以有多个 Action,通过事件驱动彼此串联,形成一个工作流。
- **Event**:内置类型包括 `InputEvent`、`OutputEvent`、`ChatRequestEvent`、`ChatResponseEvent`、`ToolRequestEvent`、`ToolResponseEvent`,也可自定义继承 `Event`。
- **RunnerContext**:Action 方法内访问状态(`getShortTermMemory()`)、发送后续事件(`sendEvent()`)、异步执行耗时操作(`executeAsync()`)的唯一入口。

## 3. 最小可运行 Agent

```java
package com.flywhl.flinklab.e12;

import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.Action;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.context.RunnerContext;

/**
 * 最小 Agent:接收车辆信号事件,判断是否超阈值,直接产出告警事件。
 * 不含 LLM 调用(第 8/9 章再引入记忆与工具调用),先把"Action + Event"骨架跑通。
 */
public class SimpleThresholdAgent extends Agent {

    @Action(listenEvents = {InputEvent.class})
    public void checkThreshold(Event event, RunnerContext ctx) throws Exception {
        InputEvent in = (InputEvent) event;
        VehicleSignal signal = (VehicleSignal) in.getInput();

        if (signal.value > signal.threshold) {
            // sendEvent 必须在 mailbox 线程内调用(单线程模型保证状态一致性)
            ctx.sendEvent(new OutputEvent(
                    "ALERT vin=%s signal=%s value=%.1f".formatted(
                            signal.vin, signal.type, signal.value)));
        }
    }

    public static class VehicleSignal {
        public String vin, type;
        public double value, threshold;
    }
}
```

```java
// 装配:把 Agent 接入 Flink 流拓扑(本地 LocalRunner 测试模式)
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
AgentsExecutionEnvironment agentsEnv = AgentsExecutionEnvironment.getExecutionEnvironment(env);

DataStream<VehicleSignal> signals = /* 数据源,参照 common/Labs 风格自定义 */;
agentsEnv.fromDataStream(signals, SimpleThresholdAgent.class)
         .toDataStream()
         .print();

env.execute("e12-07-agent-quickstart");
```

## 4. 单线程 Mailbox 模型:为什么不能随便起线程

Flink Agents 的状态访问与事件发送**必须发生在 mailbox 线程**内——这是 Flink 算子模型本身的单线程处理保证在 Agent 层的延续(与 DataStream 算子不允许跨线程访问 KeyedState 是同一条纪律)。耗时操作(如调 LLM)通过 `ctx.executeAsync(supplier)` 提交到独立线程池,JDK 21+ 利用 Continuation 机制把"等待异步结果"这段挂起,不占用 mailbox 线程,恢复后再回到 mailbox 线程继续执行后续代码——对开发者呈现的是"看起来同步"的顺序代码风格,底层却是非阻塞的。这也是本仓库全程用 JDK 21 的技术原因之一(01-XX 环境要求 + 此处 Agents 异步机制的硬性依赖)。

```java
// 官方 API 目标形态(据 GitHub Discussion #429 整理,具体签名以当前版本为准)
@Action(listenEvents = {InputEvent.class})
public void handleWithLlm(Event event, RunnerContext ctx) throws Exception {
    String result = ctx.executeAsync(() -> callSlowLlm(prompt));  // 挂起,不阻塞 mailbox
    ctx.getShortTermMemory().set("last_result", result);           // 恢复后在 mailbox 线程执行
    ctx.sendEvent(new OutputEvent(result));
}
```

## 5. Demo 状态与降级路径

`examples/e12-07-agent-quickstart/` 提供上述最小 Agent 的完整 Maven 模块(需 `flink-agents-api`/`flink-agents-runtime` 依赖,版本对齐 0.3.0)。**已知限制**:沙箱无法访问 Maven Central 拉取 flink-agents 系列依赖,本模块**未做编译验证**;JDK 21 运行需在启动参数追加 `--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED`(官方文档要求)。降级路径:若 Agents 依赖无法获取或 API 签名已变化,可用 e03-C7(Broadcast State)+ e11(Async I/O)手工搭建等价的"事件驱动决策+异步外呼"骨架,牺牲 Agents 提供的 Action 编排语法糖,保留核心的事件驱动+exactly-once语义。

## 6. 踩坑

| 坑 | 现象 | 解法 |
|---|---|---|
| 忘记追加 JVM 参数 | JDK21+ 下 Continuation 相关报错 | 按官方文档在 `config.yaml` 的 `env.java.opts.all` 追加 `--add-exports` |
| 在 executeAsync 外做状态访问 | 并发问题,状态不一致 | 状态访问/事件发送严格限定在 mailbox 线程(executeAsync 外的代码) |
| Action State Store 未显式配置 | 0.3 起无隐式默认后端,启动失败 | 显式配置 Kafka 或 Fluss 作为 action state store |

## 7. 最佳实践

- 新 Agent 一律先用 LocalRunner 单进程调试通过,再切 ActionExecutionOperator 部署到 Flink 集群。
- Action 方法保持单一职责(一个 Action 只做一件事,通过事件串联多个 Action),避免把整个业务逻辑塞进一个方法。

## 8. 面试题

① 为什么 Flink Agents 的状态访问必须限定在 mailbox 线程?② LocalRunner 与 ActionExecutionOperator 两种执行模式的适用场景?③ 0.3 版本为什么移除了"隐式默认" action state store?

## 9. 参考资料

Apache Flink Agents 0.3.0/0.2.0 Release Announcement;GitHub Discussion #429(Java 异步执行设计);apache/flink-agents 官方 Quickstart(Workflow Agent);DeepWiki apache/flink-agents 架构总览。
