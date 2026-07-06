# e12-08 · Streaming Memory:短期记忆读写(standalone · Preview)

> 对应 [ai/chapters/08-streaming-memory.md](../../ai/chapters/08-streaming-memory.md) · Level:L5
> ⚠️ 独立 Maven 工程,不进父 pom 聚合(理由同 e12-07:Preview 依赖隔离)。

## 运行方式

```bash
cd examples/e12-08-streaming-memory
mvn -q compile exec:java -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingMemoryJob \
    -Dexec.jvmArgs="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
```

## 预期现象

每个 vin 的第 1 条输出 `trend=FIRST`,第 2 条起输出 `RISING/FALLING/FLAT` 并携带 `prev=` 上一次的值——证明短期记忆跨多次 Agent 运行保留,且按 key(vin)相互隔离。

## 三层记忆覆盖说明

| 层 | 本 Demo 覆盖情况 |
|---|---|
| Sensory | 方法局部变量(隐式演示) |
| Short-Term | ✅ 核心演示对象(get/set + keyBy 隔离);TTL 配置见 ai/08 第 3 节,属集群部署配置 |
| Long-Term(Mem0) | 未演示——需外部 Mem0 服务/API Key;代码形态见 ai/08 第 4 节,0.3 起长期记忆统一收敛到 Mem0 后端 |

## 已知限制与降级路径

同 e12-07 第 1/2 条(未经编译验证、JVM 参数必需)。短期记忆场景的降级路径最直接:手写 e03-C6 风格的 `ValueState` + TTL,语义完全等价——本 Demo 的价值在于展示 Agents 框架把这套模式托管后的开发体验差异。

## 面试题

见 ai/chapters/08-streaming-memory.md 第 8 节。
