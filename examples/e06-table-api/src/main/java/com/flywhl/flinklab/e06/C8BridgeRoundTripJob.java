package com.flywhl.flinklab.e06;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * e06-C8 · 桥接往返:DataStream → Table(SQL 窗口)→ DataStream(继续 Process)。
 *
 * <p>混合架构的标准形态:SQL 干"聚合/关联"这类声明式擅长的活,回到 DataStream
 * 干"个性化时序/外呼/CEP"这类命令式擅长的活。验证点:toDataStream 出来的 Row
 * 依旧携带 rowtime 与 watermark(打印下游 process 的 currentWatermark 证明),
 * 后续还能继续开事件时间窗口 —— 时间语义全程不断链。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C8BridgeRoundTripJob
 */
public final class C8BridgeRoundTripJob {
    private C8BridgeRoundTripJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);

        DataStream<Event> events = Labs.events(env, "clicks", 20, 5, 8, 2_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(2)));

        t.createTemporaryView("clicks", t.fromDataStream(events, Schema.newBuilder()
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build()));

        Table windowed = t.sqlQuery("""
                SELECT window_start AS ws, page, COUNT(*) AS pv FROM TABLE(
                  TUMBLE(TABLE clicks, DESCRIPTOR(rowtime), INTERVAL '10' SECOND))
                GROUP BY window_start, window_end, page""");

        DataStream<Row> back = t.toDataStream(windowed);   // 仅追加,rowtime 元数据延续

        back.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Row, String>() {
                @Override
                public void processElement(Row r, Context ctx,
                                           org.apache.flink.util.Collector<String> out) {
                    out.collect("window=%s page=%s pv=%s | 下游wm=%d(时间语义未断链)"
                            .formatted(r.getField("ws"), r.getField("page"),
                                    r.getField("pv"), ctx.timerService().currentWatermark()));
                }
            })
            .uid("e06-c8-back")
            .print();

        env.execute("e06-c8-bridge-round-trip");
    }
}
