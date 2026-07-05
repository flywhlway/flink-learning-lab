package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C7 · Temporal Join:订单按**下单时刻**的汇率换算(版本表关联)。
 *
 * <p>核心语义:FOR SYSTEM_TIME AS OF o.ts —— 关联的是"o.ts 那一刻"的汇率版本,
 * 而非最新汇率;历史重算结果可复现(Regular Join 做不到)。
 * 版本表构造:changelog 上用「去重 rn=1 视图 + 事件时间」即成 versioned view,
 * 这也是 CDC 维表(e08)进 Temporal Join 的标准姿势。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C7TemporalJoinJob
 */
public final class C7TemporalJoinJob {
    private C7TemporalJoinJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE rates_raw (
                    cur_id INT, rate DOUBLE,
                    currency AS CONCAT('C', CAST(cur_id AS STRING)),
                    update_time AS LOCALTIMESTAMP,
                    WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND
                ) WITH ('connector'='datagen','rows-per-second'='1',
                        'fields.cur_id.min'='0','fields.cur_id.max'='2',
                        'fields.rate.min'='6','fields.rate.max'='8')""");

        // versioned view:主键=currency,版本=update_time
        t.executeSql("""
                CREATE VIEW versioned_rates AS
                SELECT currency, rate, update_time FROM (
                  SELECT *, ROW_NUMBER() OVER (
                      PARTITION BY currency ORDER BY update_time DESC) AS rn
                  FROM rates_raw)
                WHERE rn = 1""");

        t.executeSql("""
                CREATE TABLE orders (
                    order_id INT, cur_id INT, amount DOUBLE,
                    currency AS CONCAT('C', CAST(cur_id AS STRING)),
                    ts AS LOCALTIMESTAMP,
                    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
                ) WITH ('connector'='datagen','rows-per-second'='5',
                        'fields.order_id.min'='1','fields.order_id.max'='999',
                        'fields.cur_id.min'='0','fields.cur_id.max'='2',
                        'fields.amount.min'='10','fields.amount.max'='100')""");

        t.executeSql("""
                CREATE TABLE cnv_out (order_id INT, currency STRING, cny DOUBLE)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO cnv_out
                SELECT o.order_id, o.currency, o.amount * r.rate
                FROM orders o
                LEFT JOIN versioned_rates FOR SYSTEM_TIME AS OF o.ts AS r
                  ON o.currency = r.currency""")
         .await();
    }
}
