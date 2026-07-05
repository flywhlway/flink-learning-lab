package com.flywhl.flinklab.e06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * e06-C2 · Table → DataStream:回撤表用 toChangelogStream,拿到 RowKind。
 *
 * <p>两个出口的铁律:仅追加表 → toDataStream;有更新的表 → toChangelogStream
 * (用错直接抛异常)。拿到 Row 后 row.getKind() 即 +I/-U/+U/-D ——
 * 自定义下游(如写 Redis)就在这里按 kind 分支处理。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C2ToChangelogStreamJob
 */
public final class C2ToChangelogStreamJob {
    private C2ToChangelogStreamJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);

        t.executeSql("""
                CREATE TABLE clicks (user_id INT)
                WITH ('connector'='datagen','rows-per-second'='5',
                      'fields.user_id.min'='1','fields.user_id.max'='3')""");

        Table agg = t.sqlQuery("SELECT user_id, COUNT(*) AS pv FROM clicks GROUP BY user_id");

        t.toChangelogStream(agg)
         .map((Row r) -> "%s  user=%s pv=%s".formatted(
                 r.getKind(), r.getField("user_id"), r.getField("pv")))
         .print();

        env.execute("e06-c2-to-changelog-stream");
    }
}
