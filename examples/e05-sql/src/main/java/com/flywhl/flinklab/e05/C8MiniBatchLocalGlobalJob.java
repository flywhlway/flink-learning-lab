package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C8 · 聚合优化三开关:mini-batch + local-global 对执行计划的改写。
 *
 * <p>先打印同一查询在优化开/关下的 EXPLAIN:开启后计划里出现
 * LocalGroupAggregate → GlobalGroupAggregate 两级(预聚合吸收热点),
 * 且算子带 miniBatch 属性 —— 高基数/倾斜聚合的第一救济手段(纯配置,不改 SQL)。
 * 随后以开启态运行作业。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C8MiniBatchLocalGlobalJob
 */
public final class C8MiniBatchLocalGlobalJob {
    private C8MiniBatchLocalGlobalJob() {
    }

    private static final String QUERY =
            "SELECT user_id, COUNT(*), SUM(amount) FROM orders GROUP BY user_id";

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.executeSql("""
                CREATE TABLE orders (user_id INT, amount DOUBLE)
                WITH ('connector'='datagen','rows-per-second'='50',
                      'fields.user_id.min'='1','fields.user_id.max'='5',
                      'fields.amount.min'='1','fields.amount.max'='9')""");

        System.out.println("========== 优化关闭时的计划 ==========");
        System.out.println(t.explainSql(QUERY));

        t.getConfig().set("table.exec.mini-batch.enabled", "true");
        t.getConfig().set("table.exec.mini-batch.allow-latency", "2 s");
        t.getConfig().set("table.exec.mini-batch.size", "500");
        t.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        System.out.println("========== 优化开启时的计划(注意两级聚合)==========");
        System.out.println(t.explainSql(QUERY));

        t.executeSql("""
                CREATE TABLE agg_out (user_id INT, cnt BIGINT, total DOUBLE)
                WITH ('connector'='print')""");
        t.executeSql("INSERT INTO agg_out " + QUERY).await();
    }
}
