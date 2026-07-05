package com.flywhl.flinklab.e06;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * e06-C7 · Table API 中的函数调用:call() 与内联表达式。
 *
 * <p>Table API 用 call(Fn.class, args) 调 UDF、用内置表达式(upperCase 等)做
 * 轻转换 —— 与 SQL 注册函数是同一套函数栈,两边注册一次即可混用。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C7CallFunctionJob
 */
public final class C7CallFunctionJob {
    private C7CallFunctionJob() {
    }

    /** 风险分:金额越高分越高(演示纯函数)。 */
    public static class RiskScore extends ScalarFunction {
        public int eval(Double amount) {
            return amount == null ? 0 : (int) Math.min(100, amount / 5);
        }
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE orders (uid INT, page STRING, amount DOUBLE)
                WITH ('connector'='datagen','rows-per-second'='5',
                      'fields.uid.min'='1','fields.uid.max'='99',
                      'fields.page.length'='1',
                      'fields.amount.min'='1','fields.amount.max'='500')""");

        t.executeSql("""
                CREATE TABLE fn_out (uid INT, page STRING, risk INT)
                WITH ('connector'='print')""");

        t.from("orders")
         .select($("uid"),
                 $("page").upperCase().as("page"),
                 call(RiskScore.class, $("amount")).as("risk"))
         .filter($("risk").isGreater(60))
         .executeInsert("fn_out")
         .await();
    }
}
