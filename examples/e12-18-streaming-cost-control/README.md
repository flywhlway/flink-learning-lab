# e12-18 · Streaming Cost Control:Token 计量与预算熔断

> 对应 [ai/chapters/18-streaming-cost-control.md](../../ai/chapters/18-streaming-cost-control.md) · Level:L4
> 运行:`mvn -q -Plocal compile exec:java -pl e12-18-streaming-cost-control -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingCostControlJob`

## 背景

组合复用 e02(滚动窗口)与"计量流水"概念(生产版本落 ClickHouse,复用 e07-C6),用模拟 token 数代替真实 LLM 调用,证明"计量+窗口聚合+预算熔断"这条链路的可行性,零外部依赖。

## 验证方式

`tenant-A`(70% 流量占比)应比 `tenant-B` 更早触发 `BUDGET-EXCEEDED`;`OK` 行显示未超预算租户的窗口成本。

## 源码要点

- `BUDGET_PER_MINUTE_USD` 刻意设置得很低,便于短时间内观察到熔断触发;生产环境按真实预算设定。
- 生产版本的降级动作(第 18 章"分级熔断")本 Demo 未实现,仅输出建议文本,实际系统应联动 Broadcast 阈值(参照 e12-22 相同技术模式)与真实的模型切换/限流逻辑。

## 面试题

见 ai/chapters/18-streaming-cost-control.md 第 8 节。
