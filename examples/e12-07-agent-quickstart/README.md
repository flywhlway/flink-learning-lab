# e12-07 · Flink Agents 快速上手(standalone · Preview)

> 对应 [ai/chapters/07-agent-quickstart.md](../../ai/chapters/07-agent-quickstart.md) · Level:L5
> ⚠️ **本模块是独立 Maven 工程,不在 examples 父 pom 的 `<modules>` 聚合内**——Flink Agents 0.3.0 为 Preview,隔离于主构建之外,保证主流程 `mvn clean package` 永不被 Preview 依赖阻塞。父 pom properties 已登记 `flink.agents.version=0.3.0` 注释作为版本 SSOT。

## 运行方式

```bash
cd examples/e12-07-agent-quickstart
mvn -q compile exec:java -Dexec.mainClass=com.flywhl.flinklab.e12.AgentQuickstartJob \
    -Dexec.jvmArgs="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
```

IDE 内运行需开启 "add dependencies with provided scope to classpath"(依赖全部 provided,官方 Installation 文档模式;`flink-agents-ide-support` 即为此场景提供的运行时聚合坐标)。

## 三份文件

| 文件 | 职责 |
|---|---|
| VehicleSignal.java | 输入 POJO(事件契约) |
| SimpleThresholdAgent.java | 最小 Agent:@Action 监听 InputEvent,超阈值发 OutputEvent |
| AgentQuickstartJob.java | 装配:AgentsExecutionEnvironment 包裹 Flink 环境 |

## 已知限制与降级路径(务必阅读)

1. **未经编译验证**:沙箱环境无 Maven Central 访问,本模块代码依据官方 0.3 发布说明、Installation/Quickstart 文档与 GitHub Discussion #429 整理,`fromDataStream(...).apply(...)` 等链式调用的确切签名可能与 0.3.0 实际 API 存在偏差——首次编译报错时,以 `flink-agents-examples` 官方示例源码为准做机械调整,核心概念(Agent/@Action/Event/RunnerContext)不变。
2. **JVM 参数必须保留**:`--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED` 是 JDK 21 下 Continuation 机制的硬性要求(官方 Quickstart 明示);集群模式追加到 `config.yaml` 的 `env.java.opts.all`。
3. **Action State Store**:0.3 起无隐式默认后端;本最小示例(LocalRunner 路径)不涉及,部署到集群启用 exactly-once 时须显式配置 Kafka 或 Fluss。
4. **降级路径**:若 Agents 依赖不可用,e03-C7(Broadcast State)+ e11(Async I/O)可手工搭建等价的"事件驱动决策"骨架(ai/07 第 5 节)。

## 面试题

见 ai/chapters/07-agent-quickstart.md 第 8 节。
