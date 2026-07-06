-- e12-04 · 第一步:声明 embedding 模型(与 e12-03 的生成模型区分)。
-- 前置:本机 Ollama 已 `ollama pull bge-m3`;Milvus 已启动(cd docker && make up-ai)。
-- ⚠️ 版本演进风险同 e12-03:参数键名以当前版本官方文档为准。

CREATE MODEL text_embedder
INPUT (text STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
    'provider' = 'openai',
    'endpoint' = 'http://host.docker.internal:11434/v1',
    'model-name' = 'bge-m3',
    'task' = 'embedding'
);
