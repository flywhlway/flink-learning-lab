package com.flywhl.flinklab.e06;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * e06-C3 · Table API 表达式 DSL:同一窗口聚合的"编程式"写法。
 *
 * <p>与 SQL 等价但可编排、可复用、编译期查错(列名拼错立刻 fail)——
 * 平台代码里拼装动态逻辑时 DSL 优于字符串拼 SQL(防注入、防语法漂移)。
 * 对照:e05-C2 的 TUMBLE TVF 与此处 Tumble.over(...) 生成同一物理计划。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C3ExpressionDslJob
 */
public final class C3ExpressionDslJob {
    private C3ExpressionDslJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE clicks (
                    user_id INT, page STRING, amount DOUBLE,
                    ts AS LOCALTIMESTAMP,
                    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
                ) WITH ('connector'='datagen','rows-per-second'='10',
                        'fields.user_id.min'='1','fields.user_id.max'='50',
                        'fields.page.length'='1',
                        'fields.amount.min'='1','fields.amount.max'='99')""");

        Table result = t.from("clicks")
                .filter($("amount").isGreater(lit(5)))
                .window(Tumble.over(lit(10).seconds()).on($("ts")).as("w"))
                .groupBy($("w"), $("page"))
                .select($("page"),
                        $("w").start().as("ws"),
                        $("amount").sum().as("total"),
                        $("user_id").count().as("pv"));

        t.executeSql("""
                CREATE TABLE dsl_out (page STRING, ws TIMESTAMP(3), total DOUBLE, pv BIGINT)
                WITH ('connector'='print')""");
        result.executeInsert("dsl_out").await();
    }
}
