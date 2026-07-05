package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C5 · Interval Join:订单与支付在 ±30s 时间带内关联。
 *
 * <p>与 Regular Join 的根本差异:时间带给了 planner **状态清理的依据** ——
 * watermark 越过 `订单ts + 上界` 即可删该订单的状态,状态有界;
 * Regular Join 双侧默认永存(C6)。口径:支付晚于订单 0~30s 内算成功匹配。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C5IntervalJoinJob
 * 预期:稳定输出 o/p 同 id 的配对;长跑内存平稳(对照 C6)。
 */
public final class C5IntervalJoinJob {
    private C5IntervalJoinJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 两条流用同一序列速率生成,id 空间重叠制造可匹配对
        for (String tbl : new String[]{"orders", "payments"}) {
            t.executeSql("""
                    CREATE TABLE %s (
                        id INT, amount DOUBLE,
                        ts AS LOCALTIMESTAMP,
                        WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
                    ) WITH (
                        'connector'='datagen','rows-per-second'='8',
                        'fields.id.min'='1','fields.id.max'='40',
                        'fields.amount.min'='1','fields.amount.max'='500')"""
                    .formatted(tbl));
        }

        t.executeSql("""
                CREATE TABLE joined_out (id INT, o_amt DOUBLE, p_amt DOUBLE)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO joined_out
                SELECT o.id, o.amount, p.amount
                FROM orders o JOIN payments p ON o.id = p.id
                WHERE p.ts BETWEEN o.ts AND o.ts + INTERVAL '30' SECOND""")
         .await();
    }
}
