package com.flywhl.flinklab.e06;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * e06-C5 · Row 与 DataTypes:手工构造 changelog 流 → 表。
 *
 * <p>fromChangelogStream 允许把"自带 RowKind 的 Row 流"当作回撤源 ——
 * 这是对接自定义 CDC/消息格式(如企业内部 binlog 网关)的钥匙。
 * 本例手工发 +I/-U/+U,验证下游聚合把它们当真正的更新处理。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C5RowAndDataTypesJob
 * 预期:balance 聚合先出 100,后被更新为 250(而非追加求和 350)。
 */
public final class C5RowAndDataTypesJob {
    private C5RowAndDataTypesJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);

        DataStream<Row> changelog = env.fromData(
                Row.ofKind(RowKind.INSERT,        "acct-1", 100.0),
                Row.ofKind(RowKind.INSERT,        "acct-2", 80.0),
                Row.ofKind(RowKind.UPDATE_BEFORE, "acct-1", 100.0),
                Row.ofKind(RowKind.UPDATE_AFTER,  "acct-1", 250.0));

        Table accounts = t.fromChangelogStream(changelog, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.DOUBLE())
                .build());
        t.createTemporaryView("accounts", accounts);

        t.toChangelogStream(
                t.sqlQuery("SELECT f0 AS acct, SUM(f1) AS balance FROM accounts GROUP BY f0"))
         .map(r -> r.getKind() + "  " + r.getField("acct") + " balance=" + r.getField("balance"))
         .print();

        env.execute("e06-c5-row-datatypes");
    }
}
