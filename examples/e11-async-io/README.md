# e11 · Async I/O 与维表富化(6 案例)

> 对应教材:docs/05-03(Lookup)与本页 · Level:L5
> 运行:`mvn -q -Plocal compile exec:java -pl e11-async-io -Dexec.mainClass=com.flywhl.flinklab.e11.<类名>`

## 1. 背景:为什么同步外呼是军规级禁忌

同步调用把算子吞吐钉死在 `并行度 / 单次延迟`:200ms 一次的调用,10 并行度 = 50 QPS 封顶,再多机器也没用。Async I/O 用"在途请求容量"(capacity)替代"线程数"作为并发单位,同样资源吞吐提升 1~2 个数量级。

## 2. 案例矩阵

| # | 类 | 主题 | 关键观察 |
|---|---|---|---|
| C1 | C1OrderedVsUnorderedJob | 有序 vs 无序 | UNORD 的 seq 乱序、吞吐高;事件时间窗口不受 unordered 影响 → 默认无序 |
| C2 | C2TimeoutRetryJob | 重试+超时+降级 | 框架级 FixedDelayRetry;timeout() 发降级记录,作业零重启(军规 12 底座) |
| C3 | C3CachedDimEnrichJob | 两级维表 | HIT 命中率爬升;缓存=加速器而非状态 |
| C4 | C4FailFastBudgetJob | 短超时预算 | BUDGET-EXCEEDED 降级 |
| C5 | C5CircuitOpenJob | 熔断短路 | SHORT-CIRCUIT 跳过外呼 |
| C6 | C6RefreshAheadCacheJob | 预刷新缓存 | LOAD/HIT/REFRESH |

## 3. 容量与背压模型

```mermaid
flowchart LR
    IN[上游] --> Q[在途队列 capacity=100]
    Q -->|asyncInvoke| EXT[(外部系统)]
    EXT -->|complete| OUT[下游]
    Q -. 队满即反压上游 .-> IN
```

capacity 的意义:**同时在途的请求上限**,也是该算子的反压阀门与(unordered 下)近似的内存占用上界。容量 ≈ 目标QPS × P99延迟,再按外部系统限额封顶。

## 4~6. 讲解 / 踩坑 / 实践

- **真异步客户端**:FakeDimClient 是教学替身;生产用 lettuce/vertx/async-http 这类回调型客户端。"同步客户端 + 大线程池"是伪异步——容量退化回线程数,还白吃内存。
- **asyncInvoke 不做重活**:它在算子主线程(mailbox)执行,阻塞它=阻塞一切;发起请求立即返回,一切在回调里 complete。
- **重试预算 ⊂ 总超时**(C2):3 次×200ms 必须装进 3s;超时兜底走降级而非异常,否则重启风暴。
- **exactly-once 语义边界**:Async I/O 保证请求结果不丢(在途请求随 checkpoint 记录、恢复后重发),但**外部副作用可能重放**——查询类天然幂等没事;写类外呼要么幂等要么走事务 Sink(e04-C2),别塞在 Async I/O 里。
- **与 Lookup Join 的选择**:SQL 作业首选 Lookup Join(带 cache 参数);DataStream 或需要重试/降级细控时用本模块方案。

## 7~8. 面试题 / 参考

① capacity 打满时发生什么?② unordered 模式下 watermark 如何保证不越过未完成请求?③ 重放会不会导致外呼重复执行,怎么设计?
参考:官方 DataStream→Async I/O(含 retry 支持);e11 源码;ai/03(同一骨架上跑 LLM)。
