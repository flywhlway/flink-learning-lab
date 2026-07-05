package com.flywhl.flinklab.e06;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * e06-C1 · DataStream → Table:携带事件时间与 watermark 进入 SQL 世界。
 *
 * <p>关键:fromDataStream 的 Schema 里 ① 用 TO_TIMESTAMP_LTZ 把 long ts 变
 * rowtime 列;② SOURCE_WATERMARK() 声明"沿用 DataStream 已分配的 watermark"。
 * 少了这两步,Table 侧窗口永不触发 —— 桥接故障第一名。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C1FromDataStreamJob
 */
public final class C1FromDataStreamJob {
    private C1FromDataStreamJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);

        DataStream<Event> events = Labs.events(env, "clicks", 20, 5, 8, 2_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(2)));

        Table tbl = t.fromDataStream(events, Schema.newBuilder()
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build());
        t.createTemporaryView("clicks", tbl);

        t.executeSql("""
                CREATE TABLE win_out (ws TIMESTAMP_LTZ(3), page STRING, pv BIGINT)
                WITH ('connector'='print')""");
        t.executeSql("""
                INSERT INTO win_out
                SELECT window_start, page, COUNT(*) FROM TABLE(
                  TUMBLE(TABLE clicks, DESCRIPTOR(rowtime), INTERVAL '10' SECOND))
                GROUP BY window_start, window_end, page""")
         .await();
    }
}
