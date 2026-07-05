-- e09-04 · Iceberg Catalog:同一份数据的另一种湖仓格式表达。
--
-- 前置:ADD JAR '/opt/flink/usrlib/iceberg-flink-runtime-2.2-1.7.1.jar';
-- Iceberg 的核心抽象是"表快照 = manifest 列表 = 一组 data file 的元数据清单",
-- 与 Paimon 的 LSM 主键表设计哲学不同(Iceberg 原生更偏"仅追加+定期改写",
-- 主键/CDC 更新能力通过 v2 表格式的 equality/position delete 文件实现,
-- 相对 Paimon 原生 LSM 更新链路更重)。

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 's3://warehouse/iceberg',
    's3.endpoint' = 'http://minio:9000'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

CREATE TABLE IF NOT EXISTS orders_iceberg (
    order_id BIGINT, user_id INT, page STRING, amount DOUBLE, status STRING
);

CREATE TEMPORARY TABLE orders_src2 (
    order_id BIGINT, user_id INT, page STRING, amount DOUBLE, status STRING
) WITH (
    'connector' = 'datagen', 'rows-per-second' = '10',
    'fields.order_id.min' = '1', 'fields.order_id.max' = '9999'
);

INSERT INTO orders_iceberg SELECT * FROM orders_src2;

-- Iceberg 的 Time Travel(等价能力,语法略有差异)
SELECT * FROM orders_iceberg FOR SYSTEM_TIME AS OF TIMESTAMP '2026-07-05 00:00:00';
