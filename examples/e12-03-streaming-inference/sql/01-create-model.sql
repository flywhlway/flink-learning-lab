-- e12-03 · 第一步:声明模型端点(CREATE MODEL)。
-- 前置:本机 Ollama 已启动(建议 0.9.0+)且已 `ollama pull qwen3:8b`;
--       Flink 2.2 SQL AI 函数支持(见 ai/chapters/03 第 4 节版本演进风险说明)。
-- 在 SQL Client 内执行:cd docker && make sql,然后逐段粘贴。
--
-- ⚠️ 版本演进风险:CREATE MODEL 的 WITH 参数(provider/endpoint/task 等键名)
-- 在 Flink 2.1→2.2→2.3 间仍在调整,执行前请对照当前版本官方文档
-- (SQL → Functions → Model Inference)核对参数名;本脚本以 2.2 文档语法编写。
-- 降级路径:若本版本不支持,改用 e11 Async I/O 手工调用 Ollama HTTP 接口。

CREATE MODEL risk_classifier
INPUT (review_text STRING)
OUTPUT (risk_level STRING)
WITH (
    'provider' = 'openai',                                        -- Ollama 兼容 OpenAI API 格式
    'endpoint' = 'http://host.docker.internal:11434/v1',          -- 宿主机 Ollama(OrbStack 域名)
    'model-name' = 'qwen3:8b',
    'task' = 'text_generation',
    'system-prompt' = '你是风控分类器。对输入文本输出且仅输出一个词:HIGH、MEDIUM 或 LOW。'
);
