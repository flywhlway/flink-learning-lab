package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C2 · 窗口 TVF 家族:同一数据源上 TUMBLE / HOP / CUMULATE 三种窗口并行输出。
 *
 * <p>要点:① TVF 是 1.13+ 的标准窗口写法;② HOP 的输出量 = TUMBLE ×(size/slide),
 * 状态也同倍膨胀(docs/02-03 的 SQL 版证据);③ CUMULATE 专治"当日累计到当前"这类
 * 大屏需求 —— 用 HOP 模拟它是常见的性能事故。
 * ④ 多路输出用 StatementSet:一次提交、共享源、单作业。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C2WindowTvfTrioJob
 * 预期:三种前缀表各按 10s / 10s滑5s / 累计到30s 的节奏出行。
 */
public final class C2WindowTvfTrioJob {
    private C2WindowTvfTrioJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE clicks (
                    user_id INT, page STRING,
                    ts AS LOCALTIMESTAMP,
                    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
                ) WITH (
                    'connector'='datagen','rows-per-second'='20',
                    'fields.user_id.min'='1','fields.user_id.max'='100',
                    'fields.page.length'='1')""");

        for (String name : new String[]{"tumble_out", "hop_out", "cumulate_out"}) {
            t.executeSql("CREATE TABLE " + name
                    + " (window_start TIMESTAMP(3), window_end TIMESTAMP(3), pv BIGINT)"
                    + " WITH ('connector'='print','print-identifier'='" + name + "')");
        }

        StatementSet set = t.createStatementSet();
        set.addInsertSql("""
                INSERT INTO tumble_out
                SELECT window_start, window_end, COUNT(*) FROM TABLE(
                  TUMBLE(TABLE clicks, DESCRIPTOR(ts), INTERVAL '10' SECOND))
                GROUP BY window_start, window_end""");
        set.addInsertSql("""
                INSERT INTO hop_out
                SELECT window_start, window_end, COUNT(*) FROM TABLE(
                  HOP(TABLE clicks, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
                GROUP BY window_start, window_end""");
        set.addInsertSql("""
                INSERT INTO cumulate_out
                SELECT window_start, window_end, COUNT(*) FROM TABLE(
                  CUMULATE(TABLE clicks, DESCRIPTOR(ts), INTERVAL '10' SECOND, INTERVAL '30' SECOND))
                GROUP BY window_start, window_end""");
        set.execute().await();
    }
}
