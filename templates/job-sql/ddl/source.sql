-- 数据源表 DDL 示例(datagen 占位,替换为真实 Kafka/CDC/JDBC 源)。
-- 命名建议:ods_<业务域>_<表名>
CREATE TABLE IF NOT EXISTS ods_events_raw (
    user_id INT,
    page    STRING,
    amount  DOUBLE,
    ts      AS LOCALTIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
) WITH (
    -- 替换点(项目方): 替换为真实 connector(kafka/upsert-kafka/paimon 等,见 e07/e09 决策图)
    'connector' = 'datagen',
    'rows-per-second' = '10'
);
