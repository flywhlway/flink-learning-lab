# 第 03 章 · Streaming Inference:ML_PREDICT 深解

> Demo:e12-03(SQL 脚本,`CREATE MODEL` + `ML_PREDICT` 对接本机 Ollama)· Level:L4

## 1. 问题:LLM 推理如何进流

传统做法是"Flink 算子里同步 HTTP 调用 LLM"——这违反军规 4(禁止同步外呼阻塞算子)。Flink 2.1 引入的 `ML_PREDICT` SQL 函数把"调用外部模型"标准化为一个**表函数**:输入表 + 模型描述符 + 待推理列描述符,输出表带推理结果列。底层实现天然走异步(e11 Async I/O 骨架的 SQL 化封装),推理失败/超时按标准降级路径处理,而不是让算子挂掉。

## 2. 架构与语法

```mermaid
flowchart LR
    T[输入表\n事件流] --> MP["ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(col))"]
    M[CREATE MODEL\n声明模型端点] --> MP
    MP --> OUT[输出表\n原始列 + 推理结果列]
```

```sql
-- 声明模型端点(指向本机 Ollama;生产环境替换为内部 AI 网关地址,见 ai/19)
CREATE MODEL risk_classifier
INPUT (review_text STRING)
OUTPUT (risk_level STRING, confidence DOUBLE)
WITH (
    'provider' = 'openai',              -- Ollama 兼容 OpenAI API 格式
    'endpoint' = 'http://host.docker.internal:11434/v1',
    'model-name' = 'qwen3:8b',
    'task' = 'classification'
);

-- 流式推理:每条事件到达即异步调用模型
SELECT t.order_id, t.review_text, p.risk_level, p.confidence
FROM orders_stream AS t,
     LATERAL TABLE (ML_PREDICT(t, MODEL risk_classifier, DESCRIPTOR(review_text))) AS p;
```

## 3. 工程红线(超越"能跑起来"的部分)

1. **限流**:模型端点(无论本地 Ollama 还是云端 API)都有并发上限,`ML_PREDICT` 底层的异步容量参数(对应 e11 的 capacity)必须按端点实测吞吐设置,而不是拍脑袋。
2. **超时降级**:模型推理耗时方差远大于普通外呼(P99 可能是 P50 的 10 倍),必须设置合理超时并定义降级值(如返回 `risk_level=UNKNOWN` 而非让整条流水线卡住)——与 e11-C2 的降级三件套完全同构。
3. **批化**:多数模型服务对批量请求的吞吐效率远高于单条,`ML_PREDICT` 的实现通常支持攒批调用,配置项因 Provider 而异,上线前务必实测批大小对延迟的影响。
4. **成本可见性**:每次 `ML_PREDICT` 调用的 token 消耗应能被计量(ai/18),否则一次数据回溯重跑可能产生意外的模型调用账单。

## 4. Demo 状态与降级路径

本章 Demo 为可直接在 SQL Client 执行的脚本(`examples/e12-03-streaming-inference/`),需要本机 Ollama 服务(建议 0.9.0+,模型 qwen3:8b)与 Flink 2.2 SQL AI 函数支持。**已知限制**:`ML_PREDICT` 的具体 WITH 参数因 Flink 版本与 Provider 类型存在差异,本章 SQL 以官方文档语法为准编写但未在沙箱内实际连接 Ollama 验证,请在本机执行前对照当前 Flink 版本的 SQL AI 函数文档核对参数名。降级路径:若 `ML_PREDICT` 因版本差异不可用,可退回 e11 Async I/O 的 DataStream 方案手工实现等价逻辑(RichAsyncFunction 调用 Ollama HTTP 接口)。

## 5. 踩坑

| 坑 | 现象 | 解法 |
|---|---|---|
| 未设超时 | 模型服务抖动时整条流水线背压堆积 | 显式设置合理超时+降级值 |
| 高并发打满本地 Ollama | 本机 CPU/GPU 资源耗尽,响应time劣化甚至无响应 | 按本机算力实测容量上限,不可无限并发 |
| 把推理结果当确定性输出缓存 | LLM 输出有随机性,相同输入不同输出被误判为"bug" | 明确该字段是概率性输出,下游消费方需容忍不确定性或设置 temperature=0 |

## 6. 最佳实践

- 生产环境的模型端点统一经过 AI 网关(ai/19)而非直连,便于限流、路由、成本核算集中管理。
- 每个 `CREATE MODEL` 都在设计文档里登记"降级值是什么、超时是多少、预估 QPS 与成本"。

## 7. 面试题

① `ML_PREDICT` 与手工 Async I/O 调用模型相比,标准化带来了什么、又失去了什么灵活性?② 为什么模型推理的超时方差通常远大于普通 RPC 调用?③ 批化调用如何影响端到端延迟与吞吐的权衡?

## 8. 参考资料

Flink 2.1 Release Notes(`CREATE MODEL`/`ML_PREDICT` 引入);docs/00-landscape(SQL AI 层现状);ai/19(AI Gateway 与 Model Routing)。
