# e12-04 · Streaming Embedding + Vector:流式向量化与 VECTOR_SEARCH(SQL 脚本)

> 对应 [ai/chapters/04](../../ai/chapters/04-streaming-embedding-vector.md) 与 [ai/chapters/05](../../ai/chapters/05-streaming-rag.md) · Level:L4-L5
> 形态:SQL 脚本;前置:本机 Ollama(bge-m3)+ Milvus(`cd docker && make up-ai`)。

## 执行方式

```bash
ollama pull bge-m3
cd docker && make up && make up-ai && make sql
# 依次粘贴 sql/01 → 02 → 03;05-retraction.sql 为第 5 章失效通道演示
```

## 预期现象

02 执行后 Milvus WebUI(http://localhost:9091/webui/)可见 `tickets` collection 行数增长;03 执行后每条新工单输出其 top-5 相似历史工单(冷启动初期结果少,属预期,历史向量需要积累)。

## 版本演进风险与降级路径(务必阅读)

1. **Milvus 连接器**:Flink 官方向量存储连接器在 2.2 系列仍快速演进,`WITH` 参数为示意写法,执行前对照当前版本文档;连接器不可用时降级为 e11 Async I/O + Milvus Java SDK(流式 upsert 与检索均可等价实现)。
2. **embedding 模型一致性红线**(ai/04 要点 1):写入与检索必须用同一 embedding 模型版本,换模型必须重建索引——这是向量系统最隐蔽的事故模式。
3. `05-retraction.sql` 的软删除是普适方案;若连接器支持 DELETE 语义,物理删除+软删除双保险更稳。

## 与 e12-03 的关系

e12-03 验证"文本→生成式推理"管道;本模块验证"文本→向量→检索"管道。两者共享 CREATE MODEL 机制,区别只在 task 类型与下游用法。
