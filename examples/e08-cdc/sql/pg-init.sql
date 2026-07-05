-- e08 CDC 前置:PostgreSQL 逻辑解码配置。
-- docker/docker-compose.yml 中 postgres 已设 wal_level=logical(重启后生效)。
-- 以下语句在目标库(flinklab)内执行一次:
--   docker compose exec postgres psql -U flinklab -d flinklab -f /path/to/pg-init.sql

CREATE TABLE IF NOT EXISTS orders (
    order_id    BIGINT PRIMARY KEY,
    user_id     INT NOT NULL,
    page        VARCHAR(32),
    amount      DOUBLE PRECISION,
    status      VARCHAR(16) DEFAULT 'CREATED',
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_pii (
    order_id    BIGINT PRIMARY KEY,
    phone       VARCHAR(20),
    id_card     VARCHAR(20)
);

-- REPLICA IDENTITY FULL:让 UPDATE/DELETE 的 WAL 记录携带整行旧值,
-- 否则 CDC 只能拿到主键,业务列的"更新前"值不可得(下游对账/审计会缺字段)。
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_pii REPLICA IDENTITY FULL;

-- 发布(publication):Postgres 逻辑复制的订阅单位,CDC 连接器按此过滤表。
DROP PUBLICATION IF EXISTS flink_cdc_pub;
CREATE PUBLICATION flink_cdc_pub FOR TABLE orders, order_pii;

-- 造一批初始数据,供 C1 全量+增量快照阶段观察
INSERT INTO orders (order_id, user_id, page, amount, status)
SELECT g, (g % 20) + 1, '/pay', (g % 500) + 1, 'CREATED'
FROM generate_series(1, 100) AS g
ON CONFLICT DO NOTHING;
