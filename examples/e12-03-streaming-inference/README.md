# e12-03 · Streaming Inference:CREATE MODEL + ML_PREDICT(SQL 脚本)

> 对应 [ai/chapters/03-streaming-inference.md](../../ai/chapters/03-streaming-inference.md) · Level:L4
> 形态:SQL 脚本(不进 Maven modules,与 e09 同类);前置:本机 Ollama + qwen3:8b。

## 执行方式

```bash
ollama pull qwen3:8b && ollama serve       # 宿主机(若未常驻)
cd docker && make up && make sql           # 进入 SQL Client
# 依次粘贴 sql/01-create-model.sql、sql/02-streaming-predict.sql
```

## 预期现象

每秒 1 条评论文本流入,数秒内输出携带 `risk_level` 列(HIGH/MEDIUM/LOW)。首次调用因模型加载会明显偏慢(本机 Ollama 冷启动),属预期。

## 版本演进风险与降级路径(务必阅读)

1. `CREATE MODEL` 的 WITH 参数键名在 Flink 2.1→2.3 间存在调整,执行报错时先对照当前版本官方文档(SQL → Model Inference)核对参数名,而不是怀疑 Ollama。
2. datagen 生成的随机文本对分类器没有真实语义,本 Demo 验证的是**管道贯通**而非分类效果;接真实数据源(Kafka topic)后才有业务意义。
3. 降级路径:若 SQL AI 函数在当前版本不可用,用 e11 Async I/O(RichAsyncFunction + Ollama HTTP `/api/chat`)手工实现等价逻辑——ai/03 第 4 节有完整论述。

## 工程红线回顾(ai/03 第 3 节)

限流(rows-per-second 已示范)、超时降级、批化、成本可见性——四条红线在把本脚本改造成生产作业前必须逐条落实。
