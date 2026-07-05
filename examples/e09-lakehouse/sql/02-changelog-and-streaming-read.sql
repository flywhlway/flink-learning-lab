-- e09-02 · changelog-producer 与流式读:湖表也能当"流"来读。
--
-- 三种 changelog-producer 模式的选择决定下游流式读到的信息量:
--   'none'      不产出独立 changelog,流读只见 +I/+U(丢失 -U,下游无法做精确回撤计算)
--   'input'     直接把输入的 changelog(若输入本身含 -U/+U)透传下游(01 用的模式)
--   'lookup'    读时通过 lookup 旧值构造完整 -U/+U(输入是纯 append 也能补出回撤)
--   'full-compaction' compaction 时产出完整 changelog,延迟换正确性,大表常用
--
-- 关键认知:Paimon 表既能被"批读"(取当前快照全量),也能被"流读"
-- (持续消费其变更),这正是"流批一体"在存储层的落地——同一张表服务两种读法,
-- 不需要为流和批各建一份数据。

USE CATALOG paimon_catalog;
USE ods;

-- 流式读:像消费 Kafka 一样消费 Paimon 表的变更
SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY TABLE stream_print (
    order_id BIGINT, user_id INT, page STRING, amount DOUBLE, status STRING
) WITH ('connector' = 'print');

INSERT INTO stream_print
SELECT * FROM orders_pk /*+ OPTIONS('scan.mode'='latest') */;

-- 对照:批式读只看当前快照,不持续消费
-- SET 'execution.runtime-mode' = 'batch';
-- SELECT * FROM orders_pk;
