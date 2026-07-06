-- e12-03 · 第二步:流式推理(ML_PREDICT)。
-- 每条事件到达即异步调用模型,输出携带推理结果列。

CREATE TABLE reviews_stream (
    order_id INT,
    review_text STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',          -- 刻意放慢:本机 Ollama 的吞吐有限(ai/03 红线 1:限流)
    'fields.order_id.min' = '1',
    'fields.order_id.max' = '9999',
    'fields.review_text.length' = '20'
);

-- LATERAL TABLE + ML_PREDICT:对每行做模型推理
SELECT t.order_id, t.review_text, p.risk_level
FROM reviews_stream AS t,
     LATERAL TABLE (ML_PREDICT(TABLE t, MODEL risk_classifier, DESCRIPTOR(review_text))) AS p;
