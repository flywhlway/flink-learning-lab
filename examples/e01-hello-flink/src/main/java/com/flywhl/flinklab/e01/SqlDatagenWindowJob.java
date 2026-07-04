package com.flywhl.flinklab.e01;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e01-J3 · 纯 SQL 作业:datagen 源 → 10s Tumbling Window TVF → print。
 *
 * <p>意义:同一个"窗口聚合"需求,J1 用 DataStream 写了 80 行,SQL 只要 20 行 ——
 * 这是 Level 4 的核心论点:企业 70% 的流处理需求应优先用 SQL 承载。
 *
 * <p>本地运行:
 * <pre>
 * cd examples
 * mvn -q -Plocal compile exec:java -pl e01-hello-flink \
 *     -Dexec.mainClass=com.flywhl.flinklab.e01.SqlDatagenWindowJob
 * </pre>
 * 同一套 SQL 也可以直接贴进 SQL Client(make sql)执行,见 playground/。
 */
public final class SqlDatagenWindowJob {

    private SqlDatagenWindowJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql("""
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
                )""");

        tEnv.executeSql("""
                CREATE TABLE agg_print (
                    window_start TIMESTAMP(3),
                    window_end   TIMESTAMP(3),
                    page         STRING,
                    pv           BIGINT,
                    uv           BIGINT
                ) WITH ('connector' = 'print')""");

        // 窗口 TVF(Flink 1.13+ 的标准窗口写法,取代旧式 GROUP BY TUMBLE(...))
        tEnv.executeSql("""
                INSERT INTO agg_print
                SELECT window_start, window_end, page,
                       COUNT(*)                AS pv,
                       COUNT(DISTINCT user_id) AS uv
                FROM TABLE(
                    TUMBLE(TABLE clicks_gen, DESCRIPTOR(ts), INTERVAL '10' SECOND))
                GROUP BY window_start, window_end, page""")
            .await(); // 流式 INSERT 是长驻任务,await() 让 main 线程挂住
    }
}
