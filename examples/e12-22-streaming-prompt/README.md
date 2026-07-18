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
