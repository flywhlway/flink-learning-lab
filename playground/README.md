# playground/ · Flink SQL Client 交互实验场

入口:`cd docker && make sql`(进入容器内 SQL Client)。以下练习可直接整段粘贴。

## 练习 P-01:datagen → 窗口 TVF(与 e01-J3 同构,纯 SQL 视角)

```sql
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE clicks_gen (
    user_id INT,
    page    STRING,
    ts AS LOCALTIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '20',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '500',
    'fields.page.length' = '2'
);

SELECT window_start, window_end, page,
       COUNT(*) AS pv, COUNT(DISTINCT user_id) AS uv
FROM TABLE(TUMBLE(TABLE clicks_gen, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, page;
```

预期:每 10 秒滚出一批窗口行;`Ctrl+C` 退出查询不影响集群。

## 练习 P-02:观察 changelog(回撤流长什么样)

```sql
-- 无窗口的聚合会产生 -U/+U 回撤,这是理解 Flink SQL 的第一道门
SELECT page, COUNT(*) AS pv FROM clicks_gen GROUP BY page;
```

预期:tableau 模式下 op 列出现 `-U`/`+U` 交替 —— 把这幅画面刻进脑子,docs/05-01 的一切从这里开始。

## 练习 P-03:读 e01-J2 的产出(端到端打通验证)

前置:e01-J2 已提交、生成器在跑。

```sql
CREATE TABLE clicks_agg (
    window_start BIGINT, window_end BIGINT,
    page STRING, clicks BIGINT, users BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks.agg',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
SELECT * FROM clicks_agg;
```

> 注:kafka SQL connector 的 jar 需在容器内可见,Phase 2 的 e05 会把 `flink-sql-connector-kafka` 预置进 `docker/jobs` 并挂到 lib;当前若报 connector 找不到,属预期,先完成前两个练习。

## 练习 P-04:Top-N 榜单(对应 e05-C3)

```sql
CREATE TABLE orders_gen (
    user_id INT, page STRING, amount DOUBLE
) WITH ('connector'='datagen','rows-per-second'='10',
        'fields.user_id.min'='1','fields.user_id.max'='8',
        'fields.page.length'='1','fields.amount.min'='1','fields.amount.max'='100');

SELECT page, user_id, total, rn FROM (
  SELECT page, user_id, total,
         ROW_NUMBER() OVER (PARTITION BY page ORDER BY total DESC) AS rn
  FROM (SELECT page, user_id, SUM(amount) AS total FROM orders_gen GROUP BY page, user_id))
WHERE rn <= 2;
```
预期:每 page 只保留金额最高的 2 个用户;榜单变化时同一行先 `-U` 再 `+U`。

## 练习 P-05:去重保留最新(对应 e05-C4)

```sql
CREATE TABLE device_gen (
    device_id INT, temp DOUBLE, proc_time AS PROCTIME()
) WITH ('connector'='datagen','rows-per-second'='5',
        'fields.device_id.min'='1','fields.device_id.max'='3',
        'fields.temp.min'='20','fields.temp.max'='90');

SELECT device_id, temp FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY proc_time DESC) AS rn
  FROM device_gen)
WHERE rn = 1;
```
预期:每设备只保留最新一条;把 `DESC` 改 `ASC` 对比 keep-first 语义差异。

## 练习 P-06:Interval Join(对应 e05-C5)

```sql
CREATE TABLE orders_j (id INT, amount DOUBLE, ts AS LOCALTIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '2' SECOND)
WITH ('connector'='datagen','rows-per-second'='8',
      'fields.id.min'='1','fields.id.max'='40','fields.amount.min'='1','fields.amount.max'='500');
CREATE TABLE payments_j (id INT, amount DOUBLE, ts AS LOCALTIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '2' SECOND)
WITH ('connector'='datagen','rows-per-second'='8',
      'fields.id.min'='1','fields.id.max'='40','fields.amount.min'='1','fields.amount.max'='500');

SELECT o.id, o.amount, p.amount
FROM orders_j o JOIN payments_j p ON o.id = p.id
WHERE p.ts BETWEEN o.ts AND o.ts + INTERVAL '30' SECOND;
```
预期:稳定输出配对;长跑观察 Flink Web UI 状态大小平稳(对照 P-07)。

## 练习 P-07:Regular Join 状态代价(对应 e05-C6)

```sql
SET 'table.exec.state.ttl' = '1 min';

CREATE TABLE users_j (uid INT, v DOUBLE)
WITH ('connector'='datagen','rows-per-second'='5','fields.uid.min'='1','fields.uid.max'='20');
CREATE TABLE orders_j2 (uid INT, v DOUBLE)
WITH ('connector'='datagen','rows-per-second'='5','fields.uid.min'='1','fields.uid.max'='20');

SELECT u.uid, u.v, o.v FROM users_j u JOIN orders_j2 o ON u.uid = o.uid;
```
预期:不设 TTL 时状态持续增长;设了 TTL 后 1 分钟未访问的 key 状态被回收(正确性代价自行权衡)。

## 练习 P-08:Temporal Join 版本表(对应 e05-C7)

```sql
CREATE TABLE rates_raw (cur_id INT, rate DOUBLE,
    currency AS CONCAT('C', CAST(cur_id AS STRING)),
    update_time AS LOCALTIMESTAMP,
    WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND)
WITH ('connector'='datagen','rows-per-second'='1',
      'fields.cur_id.min'='0','fields.cur_id.max'='2','fields.rate.min'='6','fields.rate.max'='8');

CREATE VIEW versioned_rates AS
SELECT currency, rate, update_time FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY update_time DESC) AS rn
  FROM rates_raw) WHERE rn = 1;

CREATE TABLE orders_t (order_id INT, cur_id INT, amount DOUBLE,
    currency AS CONCAT('C', CAST(cur_id AS STRING)),
    ts AS LOCALTIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)
WITH ('connector'='datagen','rows-per-second'='5',
      'fields.order_id.min'='1','fields.order_id.max'='999',
      'fields.cur_id.min'='0','fields.cur_id.max'='2','fields.amount.min'='10','fields.amount.max'='100');

SELECT o.order_id, o.currency, o.amount * r.rate
FROM orders_t o LEFT JOIN versioned_rates FOR SYSTEM_TIME AS OF o.ts AS r
  ON o.currency = r.currency;
```
预期:换算按下单**那一刻**的汇率版本,而非查询时的最新汇率。

## 练习 P-09:EXPLAIN 三段式(对应 e05-C8/C10)

```sql
EXPLAIN SELECT user_id, COUNT(*), SUM(amount) FROM orders_gen GROUP BY user_id;
```
预期:输出 Abstract Syntax Tree / Optimized Physical Plan / Execution Plan 三段;开启 `SET 'table.exec.mini-batch.enabled'='true';` 后重新 EXPLAIN,对比两级聚合节点的出现。

## 练习 P-10:OPTIONS 与 STATE_TTL Hint(对应 e05-C10)

```sql
SELECT /*+ STATE_TTL('users_j'='2h', 'orders_j2'='30min') */ u.uid, u.v, o.v
FROM users_j /*+ OPTIONS('rows-per-second'='2') */ AS u
JOIN orders_j2 AS o ON u.uid = o.uid;
```
预期:查询级参数覆盖不影响 DDL 本身;EXPLAIN 可见 TTL 属性按表分别生效。

## 练习 P-11:UDF 与 UDTF(对应 e05-C9)

需先在 SQL Client 会话中通过 `CREATE FUNCTION` 或 Java 作业注册(纯 SQL Client 无法内联定义类),此处给出等价内置函数练习:

```sql
SELECT uid,
       CONCAT(SUBSTR(CAST(uid AS STRING),1,1), '****') AS masked
FROM orders_gen;
```
预期:体会脱敏表达式的写法;e05-C9 展示的是同等逻辑封装成可复用 UDF 的形态。

## 练习 P-12:桥接后回读(对应 e06-C1/C8 的 SQL 视角)

```sql
-- 在 SQL Client 中无法直接演示 DataStream 桥接,但可验证桥接产出的表行为一致:
SELECT window_start, page, COUNT(*) AS pv
FROM TABLE(TUMBLE(TABLE clicks_gen, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, page;
```
预期:与 e06-C1 桥接后在 DataStream 侧看到的窗口结果一致 —— 证明"进 Table 世界前后语义不变"。

## 练习 P-13:Catalog 与三级命名空间(对应 e06-C6)

```sql
SHOW CATALOGS;
SHOW DATABASES;
SHOW TABLES;
SELECT * FROM default_catalog.default_database.clicks_gen LIMIT 5;
```
预期:体会全限定名 `catalog.database.table` 的写法;换 Catalog 后重复执行,观察解析结果切换。

## 练习 P-14:upsert-kafka(对应 e07-C8)

```sql
CREATE TABLE user_pv_upsert (
    user_id INT, pv BIGINT, PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='user.pv.latest',
        'properties.bootstrap.servers'='kafka:9092',
        'key.format'='json','value.format'='json');

INSERT INTO user_pv_upsert
SELECT user_id, COUNT(*) FROM clicks_gen GROUP BY user_id;
```
另开一个 SQL Client 会话验证读回:
```sql
CREATE TABLE user_pv_read (
    user_id INT, pv BIGINT, PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='user.pv.latest',
        'properties.bootstrap.servers'='kafka:9092',
        'key.format'='json','value.format'='json');
SELECT * FROM user_pv_read;
```
预期:读回得到的是"当前最新值表",而非消息队列的历史流水。

## 练习 P-15:JDBC Lookup Join(对应 e07-C2)

前置:执行 e07-C2 javadoc 中的 PG 建表语句。
```sql
CREATE TABLE clicks_lk (page STRING, proc_time AS PROCTIME())
WITH ('connector'='datagen','rows-per-second'='10','fields.page.length'='1');

CREATE TABLE pg_dim (page STRING, owner STRING)
WITH ('connector'='jdbc','url'='jdbc:postgresql://postgres:5432/flinklab',
      'table-name'='dim_page','username'='flinklab','password'='flinklab123',
      'lookup.cache'='PARTIAL','lookup.partial-cache.max-rows'='1000',
      'lookup.partial-cache.expire-after-write'='1 min');

SELECT c.page, COALESCE(d.owner, '未认领')
FROM clicks_lk c LEFT JOIN pg_dim FOR SYSTEM_TIME AS OF c.proc_time AS d
  ON c.page = d.page;
```
预期:更新 PG 里 `dim_page` 一行,1 分钟缓存过期后查询结果才会变化——体会缓存 TTL 与更新可见性的关系。

## 练习 P-16:CDC 表读取(对应 e08)

前置:执行 `e08-cdc/sql/pg-init.sql`,并已启动 `pipelines/p01-pg-to-kafka.yaml`。
```sql
CREATE TABLE cdc_orders_read (
    order_id BIGINT, user_id INT, page STRING, amount DOUBLE, status STRING
) WITH ('connector'='kafka','topic'='cdc.orders',
        'properties.bootstrap.servers'='kafka:9092',
        'scan.startup.mode'='earliest-offset','format'='debezium-json');
SELECT * FROM cdc_orders_read;
```
预期:先看到 100 条全量快照(op=r),随后 `UPDATE orders SET status='PAID' WHERE order_id=1` 会实时出现一条 op=u 记录。

## 练习 P-17:Paimon Catalog 建表(对应 e09-01)

```sql
ADD JAR '/opt/flink/usrlib/paimon-flink-2.2.jar';
CREATE CATALOG paimon_catalog WITH (
    'type'='paimon','warehouse'='s3://warehouse/paimon',
    's3.endpoint'='http://minio:9000');
USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;
CREATE TABLE IF NOT EXISTS orders_pk (
    order_id BIGINT, user_id INT, amount DOUBLE,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket'='4');
```
预期:建表成功;完整流程见 `e09-lakehouse/sql/00-setup.sql` 起始的五个脚本。

## 练习 P-18:Paimon 流读(对应 e09-02)

```sql
USE CATALOG paimon_catalog; USE ods;
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM orders_pk /*+ OPTIONS('scan.mode'='latest') */;
```
预期:像消费 Kafka 一样持续消费 Paimon 表的变更——体会"同一张表既能批读也能流读"。

## 练习 P-19:CEP 转化漏斗(对应 e10-C2,SQL 版对照 MATCH_RECOGNIZE)

```sql
SELECT * FROM clicks_gen
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY ts
    MEASURES A.page AS browse_page, B.page AS pay_page
    PATTERN (A B) WITHIN INTERVAL '15' SECOND
    DEFINE
        A AS A.page = '/item',
        B AS B.page = '/pay'
) AS T;
```
预期:SQL 内置的 `MATCH_RECOGNIZE` 是 CEP 能力的 SQL 化表达(与 e10-C2 DataStream CEP 同源思想,写法不同);体会声明式模式匹配的可读性。

## 练习 P-20:综合演练——从 CDC 到湖仓的完整链路

```sql
-- 1) 确认 CDC 数据已流入 cdc.orders(P-16)
-- 2) 建 Paimon 主键表接收该流(整合 e08+e09)
USE CATALOG paimon_catalog; USE ods;
CREATE TABLE IF NOT EXISTS orders_from_cdc (
    order_id BIGINT, user_id INT, page STRING, amount DOUBLE, status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket'='4');

INSERT INTO orders_from_cdc
SELECT order_id, user_id, page, amount, status FROM default_catalog.default_database.cdc_orders_read;
```
预期:PG 源表的任何增删改,最终都能在 Paimon 湖表里查到最新状态——这是 CDC(e08)与湖仓(e09)串联的完整闭环,也是案例二/三数据链路的雏形。

---
全部 20 个练习覆盖 docs/01~10 的核心概念;建议按编号顺序做一遍,每题只需理解"预期现象"背后的语义,不必强记 SQL 语法细节。
