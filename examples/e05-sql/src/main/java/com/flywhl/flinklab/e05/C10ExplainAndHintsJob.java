package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C10 · 读计划与打提示:EXPLAIN、OPTIONS hint、STATE_TTL hint。
 *
 * <p>三件武器:
 * ① EXPLAIN(逻辑/优化后/物理三段)—— 排查"为什么慢/为什么回撤"的第一现场;
 * ② OPTIONS hint:查询级覆盖表参数(如临时调 datagen 速率),不动 DDL;
 * ③ STATE_TTL hint(1.18+):**按表粒度**给 Join 两侧设不同 TTL ——
 *    比全局 table.exec.state.ttl 精细(维表側可长、事实側可短)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C10ExplainAndHintsJob
 */
public final class C10ExplainAndHintsJob {
    private C10ExplainAndHintsJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        for (String tbl : new String[]{"a_stream", "b_stream"}) {
            t.executeSql("""
                    CREATE TABLE %s (id INT, v DOUBLE)
                    WITH ('connector'='datagen','rows-per-second'='5',
                          'fields.id.min'='1','fields.id.max'='10',
                          'fields.v.min'='0','fields.v.max'='9')""".formatted(tbl));
        }

        String hinted = """
                SELECT /*+ STATE_TTL('a'='2h', 'b'='30min') */ a.id, a.v, b.v
                FROM a_stream /*+ OPTIONS('rows-per-second'='2') */ AS a
                JOIN b_stream AS b ON a.id = b.id""";

        System.out.println("========== EXPLAIN(观察 join 节点与 TTL 属性)==========");
        System.out.println(t.explainSql(hinted));

        t.executeSql("""
                CREATE TABLE hint_out (id INT, a_v DOUBLE, b_v DOUBLE)
                WITH ('connector'='print')""");
        t.executeSql("INSERT INTO hint_out " + hinted).await();
    }
}
