package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C1 · 动态表与 changelog:亲眼看 -U/+U 回撤。
 *
 * <p>无窗口 GROUP BY 的结果表是"随流更新的动态表":每来一条数据,对应 key 的
 * 聚合值先撤旧(-U)再发新(+U)。这是 Flink SQL 全部语义的地基 ——
 * 看懂了 op 列,Top-N/去重/Join 的输出行为就全通了。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C1ChangelogBasicsJob
 * 预期:print 输出 op 列出现 +I(首次)与 -U/+U 交替(更新);永远不会出现 -D(本查询无删除)。
 */
public final class C1ChangelogBasicsJob {
    private C1ChangelogBasicsJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE clicks (
                    user_id INT, page STRING
                ) WITH (
                    'connector'='datagen','rows-per-second'='5',
                    'fields.user_id.min'='1','fields.user_id.max'='3',
                    'fields.page.length'='1')""");

        t.executeSql("""
                CREATE TABLE out_print (user_id INT, pv BIGINT)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO out_print
                SELECT user_id, COUNT(*) AS pv FROM clicks GROUP BY user_id""")
         .await();
    }
}
