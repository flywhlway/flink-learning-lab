package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C6 · Regular Join 的状态代价与 table.exec.state.ttl。
 *
 * <p>Regular Join 语义:任一侧来新数据都要跟**对侧全历史**匹配 → 双侧状态默认
 * 永不过期,这是 SQL 作业状态爆炸的第一来源(军规 11)。本例显式设置
 * `table.exec.state.ttl = 1min`:1 分钟没被访问的 join 状态被清 ——
 * 代价是"迟到超 1 分钟的匹配会漏",正确性影响必须写进需求评审。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C6RegularJoinTtlJob
 */
public final class C6RegularJoinTtlJob {
    private C6RegularJoinTtlJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.getConfig().set("table.exec.state.ttl", "1 min");   // 军规 11:显式声明并论证

        for (String tbl : new String[]{"users_stream", "orders_stream"}) {
            t.executeSql("""
                    CREATE TABLE %s (uid INT, v DOUBLE)
                    WITH ('connector'='datagen','rows-per-second'='5',
                          'fields.uid.min'='1','fields.uid.max'='20',
                          'fields.v.min'='0','fields.v.max'='9')""".formatted(tbl));
        }

        t.executeSql("""
                CREATE TABLE join_out (uid INT, u_v DOUBLE, o_v DOUBLE)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO join_out
                SELECT u.uid, u.v, o.v
                FROM users_stream u JOIN orders_stream o ON u.uid = o.uid""")
         .await();
    }
}
