package com.flywhl.flinklab.e06;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * e06-C4 · 混编:SQL 定义视图 → Table API 续接 → 再回 SQL 输出。
 *
 * <p>混编是生产常态:DDL/复杂查询用 SQL(可评审、可版本化),
 * 动态环节(按配置追加过滤/脱敏列)用 Table API。两个世界通过
 * createTemporaryView / sqlQuery / from 互相引用,优化器统一打平 —— 没有性能税。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C4MixedSqlTableJob
 */
public final class C4MixedSqlTableJob {
    private C4MixedSqlTableJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE orders (uid INT, amount DOUBLE)
                WITH ('connector'='datagen','rows-per-second'='8',
                      'fields.uid.min'='1','fields.uid.max'='9',
                      'fields.amount.min'='1','fields.amount.max'='200')""");

        // ① SQL:业务视图
        t.executeSql("CREATE VIEW big_orders AS SELECT * FROM orders WHERE amount > 50");

        // ② Table API:动态续接(想象这里由平台配置驱动)
        Table enriched = t.from("big_orders")
                .addColumns($("amount").times(0.13).as("tax"));
        t.createTemporaryView("big_orders_taxed", enriched);

        // ③ 回 SQL:聚合输出
        t.executeSql("""
                CREATE TABLE mix_out (uid INT, cnt BIGINT, tax_sum DOUBLE)
                WITH ('connector'='print')""");
        t.executeSql("""
                INSERT INTO mix_out
                SELECT uid, COUNT(*), SUM(tax) FROM big_orders_taxed GROUP BY uid""")
         .await();
    }
}
