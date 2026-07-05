package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C3 · Top-N:每页面消费额 Top2 用户(ROW_NUMBER 模式)。
 *
 * <p>要点:① 外层 `WHERE rn <= 2` 是планner 识别 Top-N 的固定形态;
 * ② 无窗口 Top-N 是**回撤型**输出(榜单变化时 -U/+U 成对出现),下游要 upsert;
 * ③ 状态 = 每 key 一个候选堆,别忘 table.exec.state.ttl(C6/C10 展开)。
 * 生产更常用「窗口 Top-N」(TVF 子查询内先聚合)——输出仅追加,状态随窗口清理。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C3TopNJob
 */
public final class C3TopNJob {
    private C3TopNJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE orders (
                    user_id INT, page STRING, amount DOUBLE
                ) WITH (
                    'connector'='datagen','rows-per-second'='10',
                    'fields.user_id.min'='1','fields.user_id.max'='8',
                    'fields.page.length'='1',
                    'fields.amount.min'='1','fields.amount.max'='100')""");

        t.executeSql("""
                CREATE TABLE top_out (page STRING, user_id INT, total DOUBLE, rn BIGINT)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO top_out
                SELECT page, user_id, total, rn FROM (
                  SELECT page, user_id, total,
                         ROW_NUMBER() OVER (PARTITION BY page ORDER BY total DESC) AS rn
                  FROM (SELECT page, user_id, SUM(amount) AS total
                        FROM orders GROUP BY page, user_id))
                WHERE rn <= 2""")
         .await();
    }
}
