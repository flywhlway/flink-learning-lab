# e12-01 · 轮询 vs 事件驱动:延迟对照实验

> 对应 [ai/chapters/01-why-streaming.md](../../ai/chapters/01-why-streaming.md) · Level:L1
> 运行:`mvn -q -Plocal compile exec:java -pl e12-01-polling-vs-event -Dexec.mainClass=com.flywhl.flinklab.e12.PollingVsEventDrivenJob`

## 背景

全书第一个论点——"Streaming 是 Agent 进入生产的前提条件"——不该只是断言,本 Demo 用同一批异常事件跑两种检测方式,把延迟差异变成可测量的数字。

## 验证方式

运行后观察两种前缀的输出:`EVENT-DRIVEN` 行的延迟恒为个位数到两位数毫秒;`POLLING` 行的延迟分布在 `[0, 2000]` 毫秒之间(轮询间隔=2秒)。多跑几次可以直观看到轮询延迟的随机性(取决于异常恰好落在轮询周期的哪个位置),而事件驱动延迟始终稳定。

## 源码要点

- 两个分支共享同一份输入事件(`Labs.events`),保证对比公平。
- 轮询分支用**处理时间定时器**模拟"每 N 秒扫一次"的批量检查行为——用 Flink 的定时器机制模拟一个本不需要 Flink 的传统轮询系统,恰好说明了"能力过剩"与"能力不足"的两端都不是好的架构选择。
- 把 `POLL_INTERVAL_MS` 改小可以观察延迟下降,但代价是定时器触发频率上升(对应真实系统里"扫描频率上升、系统负载上升"),这正是 ai/01 第 5 节论证的两难。

## 面试题

见 ai/chapters/01-why-streaming.md 第 7 节。
