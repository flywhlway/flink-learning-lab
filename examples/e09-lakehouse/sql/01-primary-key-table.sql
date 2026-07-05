-- e09-01 · Paimon 主键表:流式 upsert 的湖仓落地(解决 e05 回撤流的持久化终局)。
--
-- 主键表 = LSM 结构的可流式更新表:写入按主键 merge-on-read/merge-on-write,
-- 天然消化 -U/+U/-D(与 e07-C8 upsert-kafka 同一诉求,湖仓版答案)。
-- bucket 数决定写入并行度上限与小文件粒度,提前规划、上线后改动代价高。

CREATE TABLE IF NOT EXISTS orders_pk (
    order_id BIGINT,
    user_id  INT,
    page     STRING,
    amount   DOUBLE,
    status   STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input'   -- 详见 02:决定下游能否流式读到 -U/+U
);

-- 造一条模拟流(datagen)演示写入;实际生产由 e08 CDC 管道持续写入
CREATE TEMPORARY TABLE orders_src (
    order_id BIGINT, user_id INT, page STRING, amount DOUBLE, status STRING
) WITH (
    'connector' = 'datagen', 'rows-per-second' = '20',
    'fields.order_id.min' = '1', 'fields.order_id.max' = '50',
    'fields.user_id.min' = '1', 'fields.user_id.max' = '20',
    'fields.page.length' = '1',
    'fields.amount.min' = '1', 'fields.amount.max' = '500',
    'fields.status.length' = '1'
);

INSERT INTO orders_pk SELECT * FROM orders_src;

-- 验证:批读得到"当前最新状态"(即使 order_id 重复写入多次,表里只有一行)
SELECT COUNT(*), COUNT(DISTINCT order_id) FROM orders_pk;
